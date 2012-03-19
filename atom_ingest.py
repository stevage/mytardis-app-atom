import feedparser
import iso8601
from posixpath import basename
from tardis.tardis_portal.auth.localdb_auth import django_user
from tardis.tardis_portal.ParameterSetManager import ParameterSetManager
from tardis.tardis_portal.models import Dataset, DatasetParameter,\
    Experiment, ExperimentACL, ExperimentParameter, ParameterName, Schema, User
from django.conf import settings
import urllib2
import datetime
import logging
from celery.contrib import rdb

class AtomImportSchemas:

    BASE_NAMESPACE = 'http://mytardis.org/schemas/atom-import'


    @classmethod
    def get_schemas(cls):
        cls._load_fixture_if_necessary();
        return cls._get_all_schemas();

    @classmethod
    def get_schema(cls, schema_type=Schema.DATASET):
        cls._load_fixture_if_necessary();
        return Schema.objects.get(namespace__startswith=cls.BASE_NAMESPACE,
                                  type=schema_type)

    @classmethod
    def _load_fixture_if_necessary(cls):
        if (cls._get_all_schemas().count() == 0):
            from django.core.management import call_command
            call_command('loaddata', 'atom_ingest_schema')

    @classmethod
    def _get_all_schemas(cls):
        return Schema.objects.filter(namespace__startswith=cls.BASE_NAMESPACE)



class AtomPersister:

    # Names of parameters, must match fixture entries.
    # Some are also used for <category> processing in the feed itself.
    PARAM_ENTRY_ID = 'EntryID'
    PARAM_EXPERIMENT_ID = 'ExperimentID'
    PARAM_UPDATED = 'Updated'
    PARAM_EXPERIMENT_TITLE = 'ExperimentTitle'
    
    ALLOW_EXPERIMENT_CREATION = True         # Should we create new experiments
    ALLOW_EXPERIMENT_TITLE_MATCHING = True   # If there's no id, is the title enough to match on
    ALLOW_UNIDENTIFIED_EXPERIMENT = True   # If there's no title/id, should we process it as "uncategorized"?
    DEFAULT_UNIDENTIFIED_EXPERIMENT_TITLE="Uncategorized Data"
    ALLOW_UNNAMED_DATASETS = True            # If a dataset has no title, should we ingest it with a default name
    DEFAULT_UNNAMED_DATASET_TITLE = '(assorted files)'
    ALLOW_USER_CREATION = True               # If experiments belong to unknown users, create them?
    # Can existing datasets be updated? If not, we ignore updates. To cause a new dataset to be created, the incoming
    # feed must have a unique EntryID for the dataset (eg, hash of its contents).
    ALLOW_UPDATING_DATASETS = True
    # If a datafile is modified, do we re-harvest it (creating two copies)? Else, we ignore the update. False is not recommended.
    ALLOW_UPDATING_DATAFILES = True                     
    
    # If files are served as /user/instrument/experiment/dataset/datafile/moredatafiles
    # then 'datafile' is at depth 5. This is so we can maintain directory structure that
    # is significant within a dataset. Set to -1 to assume the last directory
    DATAFILE_DIRECTORY_DEPTH = 9 # eep

    def is_new(self, feed, entry):
        '''
        :param feed: Feed context for entry
        :param entry: Entry to check
        returns a boolean: Does a dataset for this entry already exist?
        '''
        try:
            dataset = self._get_dataset(feed, entry)
            # If datasets can be updated, we need to check if the dataset as a whole is recent, then examine datafiles later on.
            if not self.ALLOW_UPDATING_DATASETS:
                return False
            else:
                ds_updated=self._get_dataset_updated(dataset)
                if ds_updated:
                    entry_updated = iso8601.parse_date(entry.updated)
                    if entry_updated == ds_updated:
                        return False # Saved dataset same as incoming
                    elif entry_updated < ds_updated:
                        td = ds_updated - entry_updated
                        logging.getLogger(__name__).warn("Skipping dataset. Entry to ingest '{0}' is {1} *older* than dataset 'updated' record. Are the system clocks correct?".
                                                format(entry.id, self.human_time(td.days*86400 + td.seconds)))
                        return False
                    else:
                        return True
                else:
                    return True # Saved dataset has no date - need to consider updating.
            return self.ALLOW_UPDATING_DATASETS
        except Dataset.DoesNotExist:
            return True

    def _get_dataset_updated(self, dataset):
        
        from tardis.tardis_portal.util import get_local_time, get_utc_time
        import logging
        try:
            p = DatasetParameter.objects.get(
                    parameterset__dataset=dataset, 
                    parameterset__schema=AtomImportSchemas.get_schema(), 
                    name__name=self.PARAM_UPDATED)

            # Database times are naive-local, so we make them aware-local
            if p.datetime_value == None:
                logging.getLogger(__name__).warn("Weird. Dataset {0} has an UPDATED parameter with no datetime_value.".format(dataset))
                return None
            local = get_local_time(p.datetime_value)
            return local
        except DatasetParameter.DoesNotExist:
            return None
        
    def _get_dataset(self, feed, entry):
        '''
        If we have previously imported this entry as a dataset, return that dataset. Datasets
        are identified with the attached "EntryID" parameter.
        :returns the dataset corresponding to this entry, if any.
        '''
        try:
            param_name = ParameterName.objects.get(name=self.PARAM_ENTRY_ID,
                                                   schema=AtomImportSchemas.get_schema())
            # error if two datasetparameters have same details. - freak occurrence?
            parameter = DatasetParameter.objects.get(name=param_name,
                                                     string_value=entry.id)
        except DatasetParameter.DoesNotExist:
            raise Dataset.DoesNotExist
        return parameter.parameterset.dataset


    def _create_entry_parameter_set(self, dataset, entryId, updated):
        '''
        Creates or updates schema for dataset with populated 'EntryID' and 'Updated' fields
        ''' 
        schema = AtomImportSchemas.get_schema(Schema.DATASET)
        # I'm not sure why mgr.set_param always creates additional parametersets. Anyway
        # we can't use it. --SB.
        try:
            p = DatasetParameter.objects.get(parameterset__dataset=dataset, parameterset__schema=schema,
                                        name__name=self.PARAM_ENTRY_ID)
        except DatasetParameter.DoesNotExist:
            
            mgr = ParameterSetManager(parentObject=dataset, schema=schema.namespace)
            mgr.new_param(self.PARAM_ENTRY_ID, entryId)
            
        try:
            p = DatasetParameter.objects.get(parameterset__dataset=dataset, parameterset__schema=schema,
                                        name__name=self.PARAM_UPDATED)

            from tardis.tardis_portal.util import get_local_time, get_utc_time
            i=iso8601.parse_date(updated)
            l=get_local_time(i)
            u=get_utc_time(l) 
            p.datetime_value = l
            #p.updated = updated;
            p.save()
        except DatasetParameter.DoesNotExist:            
            mgr = ParameterSetManager(parentObject=dataset, schema=schema.namespace)
            mgr.new_param(self.PARAM_UPDATED, iso8601.parse_date(updated))

    def _create_experiment_id_parameter_set(self, experiment, experimentId):
        '''
        Adds ExperimentID field to dataset schema
        '''
        namespace = AtomImportSchemas.get_schema(Schema.EXPERIMENT).namespace
        mgr = ParameterSetManager(parentObject=experiment, schema=namespace)
        mgr.new_param(self.PARAM_EXPERIMENT_ID, experimentId)


    def _get_user_from_entry(self, entry):
        '''
        Finds the user corresponding to this entry, by matching email
        first, and username if that fails. May create a new user if still
        no joy.
        '''
        try:
            if entry.author_detail.email != None:
                return User.objects.get(email=entry.author_detail.email)
        except (User.DoesNotExist, AttributeError):
            pass
        try:
            return User.objects.get(username=entry.author_detail.name)
        except User.DoesNotExist:
            if not self.ALLOW_USER_CREATION:
                logging.getLogger(__name__).info("Skipping dataset. ALLOW_USER_CREATION disabled. Datasets found for user '{0}' ({1}) but user doesn't exist".format(
                        entry.author_detail.name, getattr(entry.author_detail, "email", "no email")))
                return None
        
        user = User(username=entry.author_detail.name)
        user.save()
        return user

    def human_time(self, seconds):
        mins, secs = divmod(seconds, 60)
        hours, mins = divmod(mins, 60)
        days, hours = divmod(hours, 24)
        if days > 0:
            return "%d days, %d hours" % (days, hours)
        elif hours > 0:
            return "%d hours, %d minutes" % (hours, mins)
        else:
            return '%d minutes, %d seconds' % (mins, secs)

    def process_enclosure(self, dataset, enclosure):
        '''
        Examines one "enclosure" from an entry, representing a datafile.
        Determines whether to process it, and if so, starts the transfer.
        '''
        
        filename = getattr(enclosure, 'title', basename(enclosure.href))
        # check if we were provided a full path, and hence a subdirectory for the file 
        if (self.DATAFILE_DIRECTORY_DEPTH >= 1 and
                    getattr(enclosure, "path", "") != "" and
                    enclosure.path.split("/")[self.DATAFILE_DIRECTORY_DEPTH:] != ""):
            filename = "/".join(enclosure.path.split("/")[self.DATAFILE_DIRECTORY_DEPTH:])
        
        #filename = getattr(enclosure, 'title', basename(enclosure.href))
        
        datafiles = dataset.dataset_file_set.filter(filename=filename)
        def fromunix1000 (tstr):
            return datetime.datetime.utcfromtimestamp(float(tstr)/1000)
        if datafiles.count() > 0:
            datafile = datafiles[0]
            from django.db.models import Max                     
            newest=datafiles.aggregate(Max('modification_time'))['modification_time__max']
            if not newest:# datafile.modification_time:
                ### rethink this!
                return # We have this file, it has no time/date, let's skip it.
            
            def total_seconds(td): # exists on datetime.timedelta in Python 2.7
                return (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10**6) / 10**6
            timediff = total_seconds(fromunix1000(enclosure.modified) - newest)

            if timediff == 0:
                return # We have this file already, same time/date.
            elif timediff < 0:
                logging.getLogger(__name__).warn("Skipping datafile. File to ingest '{0}' is {1} *older* than stored file. Are the system clocks correct?".
                                                format(enclosure.href, self.human_time(-timediff)))
                return
            else:
                if not self.ALLOW_UPDATING_DATAFILES:
                    logging.getLogger(__name__).warn("Skipping datafile. ALLOW_UPDATING_DATAFILES is disabled, and '{0}' is {1}newer than stored file.".
                                                format(enclosure.href, self.human_time(timediff)))
                    return
                logging.getLogger(__name__).info("Ingesting updated datafile. File to ingest '{0}' is {1} newer than stored file. This will create an additional copy.".
                                                 format(enclosure.href, self.human_time(timediff)))
        else: # no local copy already.
            logging.getLogger(__name__).info("Ingesting datafile: '{0}'".format(enclosure.href))
                
        # Create a record and start transferring.
        datafile = dataset.dataset_file_set.create(url=enclosure.href, \
                                                   filename=filename,
                                                   created_time=fromunix1000(enclosure.created),
                                                   modification_time=fromunix1000(enclosure.modified))
        try:
            datafile.mimetype = enclosure.mime
        except AttributeError:
            pass
        datafile.save()
        self.make_local_copy(datafile)


    def process_media_content(self, dataset, media_content):
        try:
            filename = basename(media_content['url'])
        except AttributeError:
            return
        datafile = dataset.dataset_file_set.create(url=media_content['url'], \
                                                   filename=filename)
        try:
            datafile.mimetype = media_content['type']
        except IndexError:
            pass
        datafile.save()
        self.make_local_copy(datafile)


    def make_local_copy(self, datafile):
        ''' Actually retrieve datafile. '''
        from .tasks import make_local_copy
        make_local_copy.delay(datafile)


    def _get_experiment_details(self, entry, user):
        ''' 
        Looks for clues in the entry to help identify what Experiment the dataset should
        be stored under. Looks for tags like "...ExperimentID", "...ExperimentTitle".
        Failing that, makes up an experiment ID/title.
        :param entry Dataset entry to match.
        :param user Previously identified User
        returns (experimentId, title) 
        '''
        try:
            # Google Picasa attributes
            if entry.has_key('gphoto_albumid'):
                return (entry.gphoto_albumid, entry.gphoto_albumtitle)
            # Standard category handling
            experimentId = None
            title = None
            # http://packages.python.org/feedparser/reference-entry-tags.html
            for tag in entry.tags:
                if tag.scheme.endswith(self.PARAM_EXPERIMENT_ID):
                    experimentId = tag.term
                if tag.scheme.endswith(self.PARAM_EXPERIMENT_TITLE):
                    title = tag.term
            
            if title == "":
                title = None
            if experimentId == "":
                experimentId = None
            # If a title is enough to match on, proceed...
            if (self.ALLOW_EXPERIMENT_TITLE_MATCHING):
                if (experimentId != None or title != None):
                    return (experimentId, title)
            # Otherwise require both Id and title
            if (experimentId != None and title != None):
                return (experimentId, title)
            
        except AttributeError:
            pass
        if (self.ALLOW_UNIDENTIFIED_EXPERIMENT):
            return (user.username+"-default", self.DEFAULT_UNIDENTIFIED_EXPERIMENT_TITLE)

        else:
            logging.getLogger(__name__).info("Skipping dataset. ALLOW_UNIDENTIFIED_EXPERIMENT disabled, so not harvesting unidentified experiment for user {0}.".format(
                        user.username))
            return (None, None)

    def _get_experiment(self, entry, user):
        '''
        Finds the Experiment corresponding to this entry.
        If it doesn't exist, a new Experiment is created and given
        an appropriate ACL for the user.
        '''
        experimentId, title = self._get_experiment_details(entry, user)
        if (experimentId, title) == (None, None):
            return None
        
        if experimentId:
            try:
                # Try and match ExperimentID if we have one.
                param_name = ParameterName.objects.\
                    get(name=self.PARAM_EXPERIMENT_ID, \
                        schema=AtomImportSchemas.get_schema(Schema.EXPERIMENT))
                parameter = ExperimentParameter.objects.\
                    get(name=param_name, string_value=experimentId)
                return parameter.parameterset.experiment
            except ExperimentParameter.DoesNotExist:
                pass
        
        # Failing that, match ExperimentTitle if possible
        if title:
            try:
                experiment = Experiment.objects.get(title=title, created_by=user)
                return experiment
            except Experiment.DoesNotExist:
                pass

        # No existing expriment - shall we create a new one?
        if not self.ALLOW_EXPERIMENT_CREATION:
            logging.getLogger(__name__).info("Skipping dataset. ALLOW_EXPERIMENT_CREATION disabled, so ignoring (experimentId: {0}, title: {1}, user: {2})".format(
                    experimentId, title, user))
            return None
        experiment = Experiment(title=title, created_by=user)
        experiment.save()
        self._create_experiment_id_parameter_set(experiment, experimentId)
        logging.getLogger(__name__).info("Created experiment {0} (title: {1}, user: {2}, experimentId: {3})".format(
                        experiment.id, experiment.title, experiment.created_by, experimentId)) 
        acl = ExperimentACL(experiment=experiment,
                pluginId=django_user,
                entityId=user.id,
                canRead=True,
                canWrite=True,
                canDelete=True,
                isOwner=True,
                aclOwnershipType=ExperimentACL.OWNER_OWNED)
        acl.save()
        return experiment


    def process(self, feed, entry):
        '''
        Processes one entry from the feed, retrieving data, and 
        saving it to an appropriate Experiment if it's new.
        :returns Saved dataset
        '''
        #import pydevd; pydevd.settrace()
        user = self._get_user_from_entry(entry)
        if not user:
            return None # No target user means no ingest.
        dataset_description=entry.title
        if not dataset_description:
            if not self.ALLOW_UNNAMED_DATASETS:
                logging.getLogger(__name__).info("Skipping dataset. ALLOW_UNNAMED_DATASETS disabled, so ignoring unnamed dataset ({0}) for user {1}".format(entry.id, user))
                return
            dataset_description=self.DEFAULT_UNNAMED_DATASET_TITLE
        
        # Create dataset if necessary
        try:
            dataset = self._get_dataset(feed, entry)
            # no exception? dataset found, so do we want to try and update it?
            if not self.ALLOW_UPDATING_DATASETS:
                logging.getLogger(__name__).debug("Skipping dataset. ALLOW_UPDATING_DATASETS disabled, so ignore existing dataset {0}".format(dataset.id))
                return dataset

        except Dataset.DoesNotExist:
            experiment = self._get_experiment(entry, user)
            if not experiment: # Experiment not found and can't be created.
                return None
            dataset = experiment.dataset_set.create(description=dataset_description)
            logging.getLogger(__name__).info("Created dataset {0} '{1}' (#{2}) in experiment {3} '{4}'".format(dataset.id, dataset.description, entry.id,
                    experiment.id, experiment.title))
            dataset.save()
            
        # Set 'Updated' parameter for dataset, and collect the files.
        self._create_entry_parameter_set(dataset, entry.id, entry.updated)
        for enclosure in getattr(entry, 'enclosures', []):
            self.process_enclosure(dataset, enclosure)
        for media_content in getattr(entry, 'media_content', []):
            self.process_media_content(dataset, media_content)
        return dataset



class AtomWalker:
    '''Logic for retrieving an Atom feed and iterating over it - but nothing about to do with the entries.'''

    def __init__(self, root_doc, persister = AtomPersister()):
        self.root_doc = root_doc
        self.persister = persister


    @staticmethod
    def get_credential_handler():
        passman = urllib2.HTTPPasswordMgrWithDefaultRealm()
        try:
            for url, username, password in settings.ATOM_FEED_CREDENTIALS:
                passman.add_password(None, url, username, password)
        except AttributeError:
            # We may not have settings.ATOM_FEED_CREDENTIALS
            pass
        handler = urllib2.HTTPBasicAuthHandler(passman)
        handler.handler_order = 490
        return handler


    @staticmethod
    def _get_next_href(doc):
        #return None #####
        try:
            links = filter(lambda x: x.rel == 'next', doc.feed.links)
            if len(links) < 1:
                return None
            return links[0].href
        except AttributeError:
            # May not have any links to filter
            return None


    def get_entries(self):
        '''
        returns list of (feed, entry) tuples to be processed, filtering out old ones.
        '''
        doc = self.fetch_feed(self.root_doc)
        entries = []
        totalentries = 0
        while True:
            if doc == None:
                break
            totalentries += len(doc.entries)
            new_entries = filter(lambda entry: self.persister.is_new(doc.feed, entry), doc.entries)
            entries.extend(map(lambda entry: (doc.feed, entry), new_entries))
            next_href = self._get_next_href(doc)
            # Stop if the filter found an existing entry or no next
            if len(new_entries) != len(doc.entries) or next_href == None:
                break
            doc = self.fetch_feed(next_href)
        logging.getLogger(__name__).info("Received feed. {0} new entries out of {1} to process.".format(len(entries), totalentries))
        return reversed(entries)

    def ingest(self): 
        '''
        Processes each of the entries in our feed.
        '''
        for feed, entry in self.get_entries():
            self.persister.process(feed, entry)


    def fetch_feed(self, url):
        '''Retrieves the current contents of our feed.'''
        return feedparser.parse(url, handlers=[self.get_credential_handler()])


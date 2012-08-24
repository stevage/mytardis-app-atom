import feedparser
import iso8601
from posixpath import basename
from tardis.tardis_portal.auth.localdb_auth import django_user
from tardis.tardis_portal.fetcher import get_credential_handler
from tardis.tardis_portal.ParameterSetManager import ParameterSetManager
from tardis.tardis_portal.models import Dataset, DatasetParameter, \
    Experiment, ExperimentACL, ExperimentParameter, ParameterName, Schema, \
    Dataset_File, User, UserProfile
from django.db import transaction
from django.conf import settings
import urllib2
import datetime
import logging
from celery.contrib import rdb
from options import IngestOptions
from tardis.tardis_portal.models import Dataset_File
from django.utils.importlib import import_module
from time_util import get_local_time, get_utc_time, get_local_time_naive

# Ensure filters are loaded
try:
    from tardis.tardis_portal.filters import FilterInitMiddleware
    FilterInitMiddleware()
except Exception:
    pass

import logging
logger = logging.getLogger(__name__)

class AtomImportSchemas:
    if IngestOptions.USE_MIDDLEWARE_FILTERS:
        modules = settings.FILTER_MIDDLEWARE
        for filter_module, filter_class in modules:
            try:
                # import filter middleware
                filter_middleware = import_module(filter_module)
                filter_init = getattr(filter_middleware, filter_class)
                # initialise filter
                
                logging.getLogger(__name__).info("Initialising middleware filter %s" % filter_module)
                filter_init()
            except ImportError, e:
                logging.getLogger(__name__).error('Error importing filter %s: "%s"' % (filter_module, e) )


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

    def is_new(self, feed, entry):
        '''
        :param feed: Feed context for entry
        :param entry: Entry to check
        returns a boolean: Does a dataset for this entry already exist?
        '''
        try:
            dataset = self._get_dataset(feed, entry)
            # If datasets can be updated, we need to check if the dataset as a whole is recent, then examine datafiles later on.
            if not IngestOptions.ALLOW_UPDATING_DATASETS:
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
            return IngestOptions.ALLOW_UPDATING_DATASETS
        except Dataset.DoesNotExist:
            return True

    def _get_dataset_updated(self, dataset):
        
        import logging
        try:
            p = DatasetParameter.objects.get(
                    parameterset__dataset=dataset, 
                    parameterset__schema=AtomImportSchemas.get_schema(), 
                    name__name=IngestOptions.PARAM_UPDATED)

            # Database times are naive-local, so we make them aware-local
            timestamp = p.datetime_value
            if timestamp == None:
                try:
                    timestamp = iso8601.parse_date(p.string_value)
                except:
                    logger.exception("Bad timestamp in Dataset {0}'s UPDATED parameter.".format(dataset))
                    return None
            local = get_local_time(timestamp)
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
            param_name = ParameterName.objects.get(name=IngestOptions.PARAM_ENTRY_ID,
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
                                        name__name=IngestOptions.PARAM_ENTRY_ID)
        except DatasetParameter.DoesNotExist:
            
            mgr = ParameterSetManager(parentObject=dataset, schema=schema.namespace)
            mgr.new_param(IngestOptions.PARAM_ENTRY_ID, entryId)
        try:
            p = DatasetParameter.objects.get(parameterset__dataset=dataset, parameterset__schema=schema,
                                        name__name=IngestOptions.PARAM_UPDATED)

            i=iso8601.parse_date(updated)
            l=get_local_time_naive(i)
            p.datetime_value = l
            p.save()
        except DatasetParameter.DoesNotExist:            
            mgr = ParameterSetManager(parentObject=dataset, schema=schema.namespace)
                       
            t = get_local_time_naive(iso8601.parse_date(updated))
            logging.getLogger(__name__).debug("Setting update parameter with datetime %s" % t)  
            mgr.new_param(IngestOptions.PARAM_UPDATED, t)

    def _create_experiment_id_parameter_set(self, experiment, experimentId):
        '''
        Adds ExperimentID field to dataset schema
        '''
        namespace = AtomImportSchemas.get_schema(Schema.EXPERIMENT).namespace
        mgr = ParameterSetManager(parentObject=experiment, schema=namespace)
        mgr.new_param(IngestOptions.PARAM_EXPERIMENT_ID, experimentId)


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
        # Handle spaces in name
        username_ = entry.author_detail.name.strip().replace(" ", "_")
        try:
            return User.objects.get(username=username_)
        except User.DoesNotExist:
            if len(username_) < 1:
                logger.warn("Skipping dataset. Dataset found with blank username.")
                retun None
            if not IngestOptions.ALLOW_USER_CREATION:
                logging.getLogger(__name__).info("Skipping dataset. ALLOW_USER_CREATION disabled. Datasets found for user '{0}' ({1}) but user doesn't exist".format(
                        entry.author_detail.name, getattr(entry.author_detail, "email", "no email")))
                return None
        
        user = User(username=username_)
        user.save()
        UserProfile(user=user).save()
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
        if (IngestOptions.DATAFILE_DIRECTORY_DEPTH >= 1 and
                    getattr(enclosure, "path", "") != "" and
                    enclosure.path.split("/")[IngestOptions.DATAFILE_DIRECTORY_DEPTH:] != ""):
            filename = "/".join(enclosure.path.split("/")[IngestOptions.DATAFILE_DIRECTORY_DEPTH:])
        
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
                if not IngestOptions.ALLOW_UPDATING_DATAFILES:
                    logging.getLogger(__name__).warn("Skipping datafile. ALLOW_UPDATING_DATAFILES is disabled, and '{0}' is {1}newer than stored file.".
                                                format(enclosure.href, self.human_time(timediff)))
                    return
                logging.getLogger(__name__).info("Ingesting updated datafile. File to ingest '{0}' is {1} newer than stored file. This will create an additional copy.".
                                                 format(enclosure.href, self.human_time(timediff)))
                if IngestOptions.HIDE_REPLACED_DATAFILES:
                    # Mark all older versions of file as hidden. (!)
                    from tardis.microtardis.models import Dataset_Hidden  
                    Dataset_Hidden.objects.filter(datafile__dataset=dataset).update(hidden=True)
        else: # no local copy already.
            logging.getLogger(__name__).info("Ingesting datafile: '{0}'".format(enclosure.href))


        # Create a record and start transferring.
        datafile = Dataset_File(dataset=dataset,
                                url=enclosure.href, 
                                filename=filename,
                                created_time=fromunix1000(enclosure.created),
                                modification_time=fromunix1000(enclosure.modified))
        datafile.protocol = enclosure.href.partition('://')[0]
        
        try:
            datafile.mimetype = enclosure.mime
        except AttributeError:
            pass

        try:
            datafile.size = enclosure.length
        except AttributeError:
            pass

        try:
            hash = enclosure.hash
            # Split on white space, then ':' to get tuples to feed into dict
            hashdict = dict([s.partition(':')[::2] for s in hash.split()])
            # Set SHA-512 sum
            datafile.sha512sum = hashdict['sha-512']
        except AttributeError:
            pass
        datafile.save()
        self.make_local_copy(datafile)

# process_media_content() used to be here?

    def make_local_copy(self, datafile):
        ''' Actually retrieve datafile. '''
        from tardis.tardis_portal.tasks import make_local_copy
        make_local_copy.delay(datafile.id)


    def _get_experiment_details(self, entry, user):
        ''' 
        Looks for clues in the entry to help identify what Experiment the dataset should
        be stored under. Looks for tags like "...ExperimentID", "...ExperimentTitle".
        Failing that, makes up an experiment ID/title.
        :param entry Dataset entry to match.
        :param user Previously identified User
        returns (experimentId, title, publicAccess) 
        '''
        try:
            # Standard category handling
            experimentId = None
            title = None
            # http://packages.python.org/feedparser/reference-entry-tags.html
            for tag in entry.tags:
                if tag.scheme.endswith(IngestOptions.PARAM_EXPERIMENT_ID):
                    experimentId = tag.term
                if tag.scheme.endswith(IngestOptions.PARAM_EXPERIMENT_TITLE):
                    title = tag.term
            
            if title == "":
                title = None
            if experimentId == "":
                experimentId = None
            # If a title is enough to match on, proceed...
            if (IngestOptions.ALLOW_EXPERIMENT_TITLE_MATCHING):
                if (experimentId != None or title != None):
                    return (experimentId, title, Experiment.PUBLIC_ACCESS_NONE)
            # Otherwise require both Id and title
            if (experimentId != None and title != None):
                return (experimentId, title, Experiment.PUBLIC_ACCESS_NONE)
            
        except AttributeError:
            pass
        if (IngestOptions.ALLOW_UNIDENTIFIED_EXPERIMENT):
            return (user.username+"-default", 
                    IngestOptions.DEFAULT_UNIDENTIFIED_EXPERIMENT_TITLE, 
                    Experiment.PUBLIC_ACCESS_NONE)

        else:
            logging.getLogger(__name__).info("Skipping dataset. ALLOW_UNIDENTIFIED_EXPERIMENT disabled, so not harvesting unidentified experiment for user {0}.".format(
                        user.username))
            return (None, None, Experiment.PUBLIC_ACCESS_NONE)

    def _get_experiment(self, entry, user):
        '''
        Finds the Experiment corresponding to this entry.
        If it doesn't exist, a new Experiment is created and given
        an appropriate ACL for the user.
        '''
        experimentId, title, public_access = self._get_experiment_details(entry, user)
        if (experimentId, title) == (None, None):
            return None
        
        if experimentId:
            try:
                # Try and match ExperimentID if we have one.
                param_name = ParameterName.objects.\
                    get(name=IngestOptions.PARAM_EXPERIMENT_ID, \
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
        if not IngestOptions.ALLOW_EXPERIMENT_CREATION:
            logging.getLogger(__name__).info("Skipping dataset. ALLOW_EXPERIMENT_CREATION disabled, so ignoring (experimentId: {0}, title: {1}, user: {2})".format(
                    experimentId, title, user))
            return None
        experiment = Experiment(title=title, created_by=user, public_access=public_access)
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

    def _lock_on_schema(self):
        schema = AtomImportSchemas.get_schema()
        Schema.objects.select_for_update().get(id=schema.id)

    def process(self, feed, entry):
        '''
        Processes one entry from the feed, retrieving data, and 
        saving it to an appropriate Experiment if it's new.
        :returns Saved dataset
        '''
        user = self._get_user_from_entry(entry)
        if not user:
            return None # No target user means no ingest.
        dataset_description=entry.title
        if not dataset_description:
            if not IngestOptions.ALLOW_UNNAMED_DATASETS:
                logging.getLogger(__name__).info("Skipping dataset. ALLOW_UNNAMED_DATASETS disabled, so ignoring unnamed dataset ({0}) for user {1}".format(entry.id, user))
                return
            dataset_description=IngestOptions.DEFAULT_UNNAMED_DATASET_TITLE
        with transaction.commit_on_success():
            # Get lock to prevent concurrent execution
            self._lock_on_schema()
            try:
                dataset = self._get_dataset(feed, entry)
                # no exception? dataset found, so do we want to try and update it?
                if not IngestOptions.ALLOW_UPDATING_DATASETS:
                    logging.getLogger(__name__).debug("Skipping dataset. ALLOW_UPDATING_DATASETS disabled, so ignore existing dataset {0}".format(dataset.id))
                    return dataset
            except Dataset.DoesNotExist:
                experiment = self._get_experiment(entry, user)
                if not experiment: # Experiment not found and can't be created.
                    return None
                dataset = experiment.datasets.create(description=dataset_description)
                logger.info("Created dataset {0} '{1}' (#{2}) in experiment {3} '{4}'".format(dataset.id, dataset.description, entry.id,
                        experiment.id, experiment.title))
                dataset.save()

                # Not allowing updating is sort of like immutability, right?
                if not IngestOptions.ALLOW_UPDATING_DATASETS:
                    dataset.immutable = True
                dataset.save()
            
            # Set 'Updated' parameter for dataset, and collect the files.
            self._create_entry_parameter_set(dataset, entry.id, entry.updated)
            for enclosure in getattr(entry, 'enclosures', []):
                try:
                    self.process_enclosure(dataset, enclosure)
                except:
                    import json
                    logging.getLogger(__name__).exception("Exception processing enclosure for {1} in dataset {0}:".format(
                            dataset, getattr(enclosure, 'href', "<no href!>")))
                    logging.getLogger(__name__).error("Enclosure: {0}".format(json.dumps(enclosure)))
            for media_content in getattr(entry, 'media_content', []):
                self.process_media_content(dataset, media_content)
            return dataset



class AtomWalker:
    '''Logic for retrieving an Atom feed and iterating over it - but nothing about to do with the entries.'''

    def __init__(self, root_doc, persister = AtomPersister()):
        self.root_doc = root_doc
        self.persister = persister


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


    def get_entries(self, full_harvest):
        '''
        full_harvest: if false, stop looking once we encounter a dataset we've previously ingested.
        This assumption means that new (unseen before) old (created a long time ago) datasets can be missed,
        if procedures allow old datasets to come online for some reason. An occasional full_harvest=True will
        catch these.
        returns list of (feed, entry) tuples to be processed, filtering out old ones.
        '''
        try: 
            doc = self.fetch_feed(self.root_doc)
            entries = []
            totalentries = 0
            if len(doc.entries) == 0:
                logging.getLogger(__name__).warn("Received feed with no entries.") 
            while True:
                if doc == None:
                    break
                totalentries += len(doc.entries)
                new_entries = filter(lambda entry: self.persister.is_new(doc.feed, entry), doc.entries)
                entries.extend(map(lambda entry: (doc.feed, entry), new_entries))
                next_href = self._get_next_href(doc)
                # Stop if the filter found an existing entry (and we're not doing a full harvest) or no next
                if (not full_harvest and len(new_entries) != len(doc.entries)) or next_href == None:
                    break
                doc = self.fetch_feed(next_href)
            logging.getLogger(__name__).info("Received feed. {0} new entries out of {1} to process.".format(len(entries), totalentries))
            return reversed(entries)       
        except: 
            logging.getLogger(__name__).exception("get_entries")
            return []

    def ingest(self, full_harvest = False): 
        '''
        Processes each of the entries in our feed.
        '''
        if full_harvest:
            logging.getLogger(__name__).info("Starting *full* harvest.")
        for feed, entry in self.get_entries(full_harvest):
            self.persister.process(feed, entry)


    def fetch_feed(self, url):
        '''Retrieves the current contents of our feed.'''
        logger.debug("Fetching feed: %s" % url)
        import urllib2
        handlers = [get_credential_handler()]
        if getattr(IngestOptions, 'HTTP_PROXY', ''):
            handlers.append (urllib2.ProxyHandler( {"http":IngestOptions.HTTP_PROXY}))
        return feedparser.parse(url, handlers=handlers)
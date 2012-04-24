class IngestOptions:

    # Names of parameters, must match fixture entries.
    # Some are also used for <category> processing in the feed itself.
    PARAM_ENTRY_ID = 'EntryID'
    PARAM_EXPERIMENT_ID = 'ExperimentID'
    PARAM_UPDATED = 'Updated'
    PARAM_EXPERIMENT_TITLE = 'ExperimentTitle'
    
    ALLOW_EXPERIMENT_CREATION = True        # Should we create new experiments
    ALLOW_EXPERIMENT_TITLE_MATCHING = True   # If there's no id, is the title enough to match on
    ALLOW_UNIDENTIFIED_EXPERIMENT = True    # If there's no title/id, should we process it as "uncategorized"?
    DEFAULT_UNIDENTIFIED_EXPERIMENT_TITLE="Uncategorized Data"
    ALLOW_UNNAMED_DATASETS = True            # If a dataset has no title, should we ingest it with a default name
    DEFAULT_UNNAMED_DATASET_TITLE = '(assorted files)'
    ALLOW_USER_CREATION = True              # If experiments belong to unknown users, create them?
    # Can existing datasets be updated? If not, we ignore updates. To cause a new dataset to be created, the incoming
    # feed must have a unique EntryID for the dataset (eg, hash of its contents).
    ALLOW_UPDATING_DATASETS = True
    # If a datafile is modified, do we re-harvest it (creating two copies)? Else, we ignore the update. False is not recommended.
    ALLOW_UPDATING_DATAFILES = True                     
    
    # If files are served as /user/instrument/experiment/dataset/datafile/moredatafiles
    # then 'datafile' is at depth 5. This is so we can maintain directory structure that
    # is significant within a dataset. Set to -1 to assume the deepest directory.
    DATAFILE_DIRECTORY_DEPTH = 6 # /mnt/rmmf_staging/e123/NovaNanoSEM/exp1/ds1/test3.tif
    USE_MIDDLEWARE_FILTERS = False # Initialise metadata extraction filters? Requires settings.py config.
    HIDE_REPLACED_DATAFILES = True # Mark old versions of updated datafiles as hidden. Requires datafile hiding feature in Tardis. 
    
    # If we can transfer files "locally" (ie, via SMB mount), then replace URL_BASE_TO_REPLACE with LOCAL_SOURCE_PATH
    # to construct a file path that can be copied from. 
    USE_LOCAL_TRANSFERS = True
    URL_BASE_TO_REPLACE = "http://datapuller.isis.rmit.edu.au/"
    LOCAL_SOURCE_PATH = "/mnt/rmmf_staging/"
    
    # Should we always examine every dataset entry in the feed, even after encountering "old" entries?
    ALWAYS_PROCESS_FULL_FEED = False
    
    HTTP_PROXY = "http://bproxy.rmit.edu.au:8080"
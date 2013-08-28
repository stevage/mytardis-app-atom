MyTardis Atom App
=================

Authors: Tim Dettrick (University of Queensland), Steve Bennett (VeRSI)

This app can be used to ingest datasets via Atom. Please see `tests/atom_test` for format examples.

New metadata is ingested first, with data files being copied asynchronously afterwards.

Installation
------------

Git clone the app into `mytardis/tardis/apps`:

[apps] git clone https://github.com/stevage/mytardis-app-atom 
[apps] mv mytardis-app-atom atom

Add it to the list of installed apps in `mytardis/settings.py`:

    INSTALLED_APPS += ('mytardis.apps.atom',)

Configuration
-------------

Celery is used to schedule periodic file ingestion. This version is designed to work with the Atom Dataset Provider (https://github.com/stevage/atom-dataset-provider), which provides a feed based on changes to a directory structure.

The `atom_ingest.walk_feeds` task takes a variable number of feeds and updates them. Here's an example 
for `settings.py` using the above dataset provider:

    CELERYBEAT_SCHEDULE = dict(CELERYBEAT_SCHEDULE.items() + {
      "update-feeds": {
          "task": "atom_ingest.walk_feeds",
          "schedule": timedelta(seconds=60),
          "args": ('http://localhost:4000/',) # Don't forget the trailing slash!
      },
    }.items())



[celerybeat][celerybeat] and [celeryd][celeryd] must be running for the scheduled updates to be performed. In
a normal MyTardis environment, they'll already be running. If not:

    bin/django celeryd --beat

HTTP Basic password protection is available via `settings.py` in MyTardis:

    REMOTE_SERVER_CREDENTIALS = [
      ('http://localhost:4272/', 'username', 'password')
    ]

In a production environment, you should combine HTTP Basic password protection with SSL for security.

Location
--------
A "location" object in Django needs to be registered for each place you're going to harvest from. Currently
it's a bit messy to do this - but hopefully we'll fix that. 

1. In the Admin interface ("yourtardissite.com/admin"), click "Add" next to "Location". Enter:
2. Name (will remain attached to ingested things as their source)
3. URL: exactly as you entered it before, but with trailing slash.
4. Type: external
5. Priority: I don't know. 10?
6. Is available: leave checked.
7. Transfer provider: Either 'local' (for file:/// copies) or 'http'.

Settings
-------------
Various policy settings are defined in options.py

    ALLOW_EXPERIMENT_CREATION = True         # Should we create new experiments
    ALLOW_EXPERIMENT_TITLE_MATCHING = True   # If there's no id, is the title enough to match on
    ALLOW_UNIDENTIFIED_EXPERIMENT = True     # If there's no title/id, should we process it as "uncategorized"?
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
    # is significant within a dataset. Set to -1 to assume the deepest directory.
    DATAFILE_DIRECTORY_DEPTH = 5

    USE_MIDDLEWARE_FILTERS = False # Initialise metadata extraction filters? Requires settings.py config.
    HIDE_REPLACED_DATAFILES = True # Mark old versions of updated datafiles as hidden. Requires datafile hiding feature in Tardis. 
    
    # If we can transfer files "locally" (ie, via SMB mount), then replace URL_BASE_TO_REPLACE with LOCAL_SOURCE_PATH
    # to construct a file path that can be copied from. 
    USE_LOCAL_TRANSFERS = True
    URL_BASE_TO_REPLACE = "http://dataprovider.example.com/files/"
    LOCAL_SOURCE_PATH = "/mnt/dataprovider/"
    
    HTTP_PROXY = "http://proxy.example.com:8080" # Leave blank for no proxy

[celerybeat]: http://ask.github.com/celery/userguide/periodic-tasks.html#starting-celerybeat
[celeryd]: http://ask.github.com/celery/userguide/workers.html#starting-the-workers

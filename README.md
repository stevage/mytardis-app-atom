MyTardis Atom App
=================

Authors: Tim Dettrick (University of Queensland), Steve Bennett (VeRSI)

This app can be used to ingest datasets via Atom. Please see `tests/atom_test` for format examples.

New metadata is ingested first, with data files being copied asynchronously afterwards.

Installation
------------

Git clone the app into `tardis/apps`:

[tardis/apps] git clone https://github.com/stevage/mytardis-app-atom 

Configuration
-------------

Celery is used to schedule periodic file ingestion. This version is designed to work with the Atom Dataset Provider (https://github.com/stevage/atom-dataset-provider), which provides a feed based on changes to a directory structure.

The `atom_ingest.walk_feeds` task takes a variable number of feeds and updates them. Here's an example 
for `settings.py` using the above dataset provider:

    CELERYBEAT_SCHEDULE = {
      "update-feeds": {
          "task": "atom_ingest.walk_feeds",
          "schedule": timedelta(seconds=60),
          "args": ('http://localhost:4000',)
      },
    }


You must run [celerybeat][celerybeat] and [celeryd][celeryd] for the scheduled updates to be performed.
MyTardis provides a `Procfile` for this purpose, but you can run both adhoc with:

    bin/django celeryd --beat

HTTP Basic password protection is available via `settings.py`:

    ATOM_FEED_CREDENTIALS = [
      ('http://localhost:4272/', 'username', 'password')
    ]

In a production environment, you should combine HTTP Basic password protection with SSL for security.

Settings
-------------
Various policy settings are defined at the top of atom_ingest.py:

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


[celerybeat]: http://ask.github.com/celery/userguide/periodic-tasks.html#starting-celerybeat
[celeryd]: http://ask.github.com/celery/userguide/workers.html#starting-the-workers
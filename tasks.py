from celery.task import task
from django.conf import settings
from django.db import DatabaseError
from tardis.tardis_portal.staging import write_uploaded_file_to_dataset
from .atom_ingest import AtomWalker
import urllib2, os
import logging

@task(name="atom_ingest.walk_feed", ignore_result=True)
def walk_feed(feed):
    try:
        AtomWalker(feed).ingest()
    except DatabaseError, exc:
        walk_feed.retry(args=[feed], exc=exc)


@task(name="atom_ingest.walk_feeds", ignore_result=True)
def walk_feeds(*feeds):
    for feed in feeds:
        walk_feed.delay(feed)


@task(name="atom_ingest.make_local_copy", ignore_result=True)
def make_local_copy(datafile):
    try:
        #opener = urllib2.build_opener((AtomWalker.get_credential_handler()))

        
        localpath = datafile.url.replace("http://datapuller.isis.rmit.edu.au/atom/", "/mnt/tardis_staging/")
        import urllib
        localpath = urllib.unquote(localpath)
        import os, pwd
        logging.getLogger(__name__).info("Locally saving file {0} to {1} as {2}".format(localpath, datafile.filename, pwd.getpwuid( os.getuid() )[ 0 ]))

        f = open(localpath)
        f_loc = write_uploaded_file_to_dataset(datafile.dataset, f, \
                                               datafile.filename)
        base_path = os.path.join(settings.FILE_STORE_PATH,
                          str(datafile.dataset.experiment.id),
                          str(datafile.dataset.id))
        datafile.url = 'tardis://' + os.path.relpath(f_loc, base_path)
        datafile.save()
    except DatabaseError, exc:
        make_local_copy.retry(args=[datafile], exc=exc)

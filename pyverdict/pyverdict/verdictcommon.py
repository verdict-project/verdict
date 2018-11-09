import json
import os
import pkg_resources
from email import message_from_string

metadata = {}

def load_metadata():
    global metadata
    root_dir = os.path.dirname(os.path.abspath(__file__))
    metadata_filename = os.path.join(root_dir, 'metadata.json')
    metadata = json.load(open(metadata_filename))

def get_metadata(name):
    load_metadata()
    return metadata[name]

def get_verdictdb_version():
    '''
    Relies on the package metadata. See below for how to read metadata.
    https://stackoverflow.com/questions/10567174/how-can-i-get-the-author-name-project-description-etc-from-a-distribution-objec
    '''
    # pkgInfo = pkg_resources.get_distribution('pyverdict').get_metadata('PKG-INFO')
    # msg = message_from_string(pkgInfo)
    # metadata = json.loads(msg['Description'])
    # verdictdb_version = metadata['Internal VerdictDB version']
    return metadata['__verdictdb_version__']

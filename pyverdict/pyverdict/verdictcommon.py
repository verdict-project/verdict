'''
    Copyright 2018 University of Michigan
 
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
'''
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

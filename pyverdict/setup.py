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
import setuptools
import os
import re
import subprocess
from build_lib import build_and_copy

pyverdict_version = '0.1.3.0'


def get_verdict_jar(lib_dir):
    lib_files = os.listdir(lib_dir)
    print(lib_files)

    def get_version(libfile):
        reobj = re.search('verdictdb-core-(.*?)-jar-with-dependencies.jar', libfile)
        if reobj is None:
            return (None, None)
        else:
            version = reobj.group(1)
            return (libfile, version)

    for libfile in lib_files:
        (libfile, version) = get_version(libfile)
        if version is not None:
            return (libfile, version)
    return (None, None)

root_dir = os.path.dirname(os.path.abspath(__file__))
lib_dir = os.path.join(root_dir, 'pyverdict', 'verdict_jar')

# if the directory does not exist, we create the directory, build a jar file, and copy the jar file
# to the lib directory.
if not os.path.exists(lib_dir):
    build_and_copy(root_dir, lib_dir)

(verdict_jar_file, verdictdb_version) = get_verdict_jar(lib_dir)
if verdictdb_version is None:
    build_and_copy(root_dir, lib_dir)
    (verdict_jar_file, verdictdb_version) = get_verdict_jar(lib_dir)


# creates a metadata file
metadata_filename = os.path.join(root_dir, 'pyverdict', 'metadata.json')
metadata_file = open(metadata_filename, 'w')
json.dump(
    {
        '__version__': pyverdict_version,
        '__verdictdb_version__': verdictdb_version
    },
    metadata_file)
metadata_file.close()


# the standard setup script
setuptools.setup(
    name='pyverdict',
    version=pyverdict_version,
    description='Python interface for VerdictDB',
    url='http://verdictdb.org',
    author='Barzan Mozafari, Yongjoo Park',
    author_email='mozafari@umich.edu, pyongjoo@umich.edu',
    license='Apache License, Version 2.0',
    packages=setuptools.find_packages(),
    package_data={'pyverdict': ['lib/*.jar', 'verdict_jar/*.jar']},
    include_package_data=True,
    install_requires=[
        'py4j >= 0.10.7',
        'numpy >= 1.9',
        'pandas >= 0.23'
    ]
)

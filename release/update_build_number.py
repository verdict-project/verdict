"""
Updates versions in:
1. The main version metadata (version.json)
2. Maven pom files
3. verdict-shell script
"""

import json
from lxml import etree
import re
import os

script_dir = os.path.dirname(os.path.realpath(__file__))
version_filename = os.path.join(script_dir, 'version.json')
main_pom_file = os.path.join(script_dir, '../pom.xml')
child_pom_files = [os.path.join(script_dir, p) for p in ['../core/pom.xml', '../jdbc/pom.xml', '../veeline/pom.xml']]
namespace = '{http://maven.apache.org/POM/4.0.0}'
veeline_bin_file = os.path.join(script_dir, '../bin/verdict-shell')

def current_version():
    return read_version(version_filename)

def read_version(file_name):
    j = None
    with open(file_name) as version_file:
        j = json.load(version_file)
    return j

def version_with_incremented_build_number(j):
    j['build'] = j['build'] + 1
    return j

def save_version_to_file(file_name, j):
    with open(file_name, 'w') as version_file:
        json.dump(j, version_file)

def version_string(j_version):
    return '%s.%s.%s' % (j_version['major'], j_version['minor'], j_version['build'])

def with_namespace(tag):
    return '%s%s' % (namespace, tag)

def pretty_print_xml(filename, tree):
    with open(filename, 'w') as f:
        f.write(etree.tostring(tree.getroot(), pretty_print=True))

def populate_version_info_in_main_pom(pom_filename, j_version):
    tree = etree.parse(pom_filename);
    tree.getroot().find(with_namespace('version')).text = version_string(j_version)
    tree.getroot().find(with_namespace('properties')).find(with_namespace('verdict.version')).text = version_string(j_version)
    pretty_print_xml(pom_filename, tree)

def populate_version_info_in_child_pom(pom_filename, j_version):
    tree = etree.parse(pom_filename)
    tree.getroot().find(with_namespace('parent')).find(with_namespace('version')).text = version_string(j_version)
    pretty_print_xml(pom_filename, tree)

def update_veeline_command(j_version):
    lines = [l for l in open(veeline_bin_file)]
    updated_lines = []
    version_string = "%s.%s.%s" % (j_version['major'], j_version['minor'], j_version['build'])
    for l in lines:
        result = re.match("VERSION=.*", l)
        if result is None:
            updated_lines.append(l)
        else:
            updated_lines.append("VERSION=%s\n" % version_string)
    with open(veeline_bin_file, 'w') as fout:
        fout.write("".join(updated_lines))

if __name__ == "__main__":
    j = read_version(version_filename)
    #j = version_with_incremented_build_number(j)
    #save_version_to_file(version_filename, j)
    populate_version_info_in_main_pom(main_pom_file, j)
    for child_pom_file in child_pom_files:
        populate_version_info_in_child_pom(child_pom_file, j)
    update_veeline_command(j)

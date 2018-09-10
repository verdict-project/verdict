import setuptools
import os
import re
import subprocess

def read_version(pom):
    """ read version info from pom.xml file
    require the version of verdictdb to be at the top of pom.xml
    """
    with open(pom) as f:
        context = f.read()
        version = re.search('<version>(.*?)</version>', context).group(1)
        return version

root_dir = os.path.dirname(os.path.abspath(__file__))
lib_dir = os.path.join(root_dir, 'pyverdict', 'lib')
if not os.path.exists(lib_dir):
    os.makedirs(lib_dir)
pom_path = os.path.join(root_dir, '..', 'pom.xml')
version = read_version(pom_path)
jar_name = 'verdictdb-core-' + version + '-jar-with-dependencies.jar'

os.chdir('..')
subprocess.check_call(['mvn','-DskipTests','-DtestPhase=false','-DpackagePhase=true','clean','package'])
subprocess.check_call(['rm', '-rf', os.path.join(lib_dir, '*verdictdb*.jar')])
subprocess.check_call(['cp', os.path.join('target', jar_name), lib_dir])
os.chdir(root_dir)

setuptools.setup(name='pyverdict',
    version=version,
    description='Python connector for VerdictDB',
    url='http://verdictdb.org',
    author='Barzan Mozafari, Yongjoo Park',
    author_email='mozafari@umich.edu, pyongjoo@umich.edu',
    license='Apache License, Version 2.0',
    packages=setuptools.find_packages(),
    package_data={'pyverdict': ['lib/*.jar']},
    include_package_data=True,
    install_requires=[
        'py4j >= 0.10.7',
        'PyMySQL >= 0.9.2',
        'pytest >= 3.5'
    ]
 )

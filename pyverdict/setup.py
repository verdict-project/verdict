from setuptools import setup
import os

setup(name='pyverdict',
      version='0.0.1',
      description='Python connector for VerdictDB',
      url='http://verdictdb.org',
      author='Barzan Mozafari, Yongjoo Park',
      author_email='mozafari@umich.edu, pyongjoo@umich.edu',
      license='Apache License, Version 2.0',
      packages=setuptools.find_packages(),
      package_data={'pyverdict': ['lib/*.jar']},
      include_package_data=True
 )

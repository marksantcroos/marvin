import os
from setuptools import setup

# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name = 'marvin-wfms',
    version = '0.1.0',
    author = 'Mark Santcroos',
    author_email = 'mark.santcroos@rutgers.edu',
    description = 'Pilot-based GWENDIA Workflow Enacter.',
    license = 'BSD',
    keywords = 'workflow gwendia pilot',
    url = 'http://packages.python.org/marvin_wfms',
    packages=['marvin'],
    long_description=read('README.md'),
    requires=['networkx', 'pykka', 'pygraphviz'],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Programming Language :: Python',
        'Topic :: Utilities',
        'License :: OSI Approved :: BSD License',
    ],
)

#!/usr/bin/env python
#
#  TODO:
#   * Figure out how to compile and install documentation automatically
#   * Add back in installation requirements
#   * Add download_url

#To enable "python setup.py develop --prefix=~/.local"
#We need to import setuptools
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(name='Jobman',
      version='hg',
      description=('Facilitate handling of many jobs'
                   '(especially jobs send on cluster)'),
      license='3-clause BSD',
      author='LISA laboratory, University of Montreal',
      author_email='theano-user@googlegroups.com',
      url='http://www.deeplearning.net/software/jobman',
      packages=['jobman', 'jobman.examples', 'jobman.analyze', 'jobman.dbi',
                ],
      scripts=['bin/jobman', 'bin/jobdispatch']

           )

#!/usr/bin/env python
#
#  TODO:
#   * Figure out how to compile and install documentation automatically
#   * Add back in installation requirements
#   * Add download_url


from distutils.core import setup

setup(name='Jobman',
      version='hg',
      description='Facilitate handling of many jobs(especially jobs send on cluster)',
      author='LISA laboratory, University of Montreal',
      author_email='theano-user@googlegroups.com',
      url='http://www.deeplearning.net/software/jobman',
      packages=['jobman', 'jobman.examples','jobman.analyze'
                ],
      scripts=['bin/jobman','bin/jobdispatch']

           )

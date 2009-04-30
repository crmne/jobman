Install Jobman
==============

One central feature of Jobman is to store all the information regarding
jobs to launch, jobs currently running, and results of finished jobs, in
one database.

In order to use that feature, you'll need to have some software installed on the central server (mainly a PostgreSQL server).

The ``jobman`` executable, that you will run on some client computer,
also has some software requirements.

Requirements on the server side
-------------------------------

A running PostgreSQL_ server, with:
  - a database, let's say ``jobbase``
  - a username and password, for instance ``jobmanager`` and ``53|<r37``

The user should have the right to connect to this database, and to
connect from a remote host if the client will run on another machine.

.. _PostgreSQL: http://www.postgresql.org/

Requirements on the client side
-------------------------------

Jobman depends on:
  - python (version?)
  - SQLAlchemy
  - psycopg2
  - tempfile?


Installation
------------

WRITEME

.. TODO: Distribute a .tgz on pylearn.org? A .egg on PyPI?


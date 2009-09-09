==============
A typical use
==============
-----------------------------------------------
Hyperparameters and results in an SQL database
-----------------------------------------------

For clarity, we'll use the ``DD`` syntax to build and use the ``state``
dictionary of parameters. See `DD` for more informations.

Design of the ``experiment`` function
======================================

First of all, you have to write a function that will set up your
experiment itself, and run the code your program consists in. This
function takes two arguments:

    - the ``state``, which is a dictionary containing the different
    parameters specifying your current experiment (for instance, the
    value of hyperparameters), and containing the results you want to
    return (for instance, the value of validation and test errors)

    - the ``channel``, which is used for communication between your
    executing job and Jobman

Let's have a look at a simple example. In this experiment, ``state``
should have at least two keys, ``first`` and ``second``, and numbers
associated to them.

.. code-block:: python

    def addition_example(state, channel):
        print 'state.first =', state.first
        print 'state.second =', state.second

        state.result = state.first + state.second

        print 'result =', state.result

        return channel.COMPLETE

Here, the code to be executed is simplistic (an addition), but
it illustrates how to retrieve arguments from ``state``, via
``state.first`` and ``state.second``, and store the results back in
``state.result``.

Note that you need to know what are the name of the keys in the
dictionary. A value in ``state`` can be another dictionary, and so on.

The last line informs Jobman that the job completed correctly.

This function must be place somewhere it can be imported from. For
instance, it can be in a directory listed in your $PYTHONPATH.

Job insertion into the database
===============================

While you can insert new jobs into the database from the command line,
the easiest way to do it is through a python program.

For instance, if you want to apply the previous experiment function to
different values of ``first`` and ``second``, like all even numbers
between 0 and 10 for ``first``, and odd numbers in the same range for
``second``.

You can then write the following code:

.. code-block:: python

    from jobman.tools import DD, flatten
    from jobman import sql

    from <somewhere> import addition_example

    TABLE_NAME='test_add_'

    # DB path...
    db = sql.db('postgres://<user>:<pass>@<server>/<database>/'+TABLE_NAME)

    state = DD()
    for first in 0,2,4,6,8,10:
        state.first = first
        for second in 1,3,5,7,9:
            state.second = second

            sql.insert_job(addition_example, flatten(state), db)

That would insert 30 jobs into the database.

Querying the database
---------------------

In order to check on the newly inserted jobs, you can directly query the
SQL database containing them. In order to have a more user-friendly view, you can execute the following python code:

.. code-block:: python

    from jobman import sql
    TABLE_NAME='test_add_'
    db = sql.db('postgres://<user>:<pass>@<server>/<database>/'+TABLE_NAME)
    db.createView(TABLE_NAME + 'view')

You can also simply add the last line at the end of the job-insertion script.

You can then log on to the database, for instance using psql command-line client :

.. code-block:: bash

    psql -h <server> -U <user> -d <database>

After entering your password, you can list the existing tables, where you should see:

.. code-block::

    <database>=> \d
                          List of relations
     Schema |             Name              |   Type   |  Owner
    --------+-------------------------------+----------+----------
    [...]
     public | test_add_keyval               | table    | <user>
     public | test_add_keyval_id_seq        | sequence | <user>
     public | test_add_trial                | table    | <user>
     public | test_add_trial_id_seq         | sequence | <user>
     public | test_add_view                 | view     | <user>
    [...]
    (31 rows)


To see the whole view of your experiments:

.. code-block::

    <database>=> SELECT * FROM test_add_view;

      id | first |               jobman_experiment               |     jobman_hash      | jobman_sql_priority | jobman_status | second 
     ----+-------+-----------------------------------------------+----------------------+---------------------+---------------+--------
       1 |     0 | jobman.examples.def_addition.addition_example |  2241733668524071315 |                   1 |             0 |      1
       2 |     0 | jobman.examples.def_addition.addition_example |  -267140279470343327 |                   1 |             0 |      3
       3 |     0 | jobman.examples.def_addition.addition_example | -6865789780955143209 |                   1 |             0 |      5
       4 |     0 | jobman.examples.def_addition.addition_example | -2040929596669704635 |                   1 |             0 |      7
       5 |     0 | jobman.examples.def_addition.addition_example | -3750366477946382133 |                   1 |             0 |      9
       6 |     2 | jobman.examples.def_addition.addition_example |  2241733668522071305 |                   1 |             0 |      1
       7 |     2 | jobman.examples.def_addition.addition_example |  -267140279468343317 |                   1 |             0 |      3
       8 |     2 | jobman.examples.def_addition.addition_example | -6865789780957143219 |                   1 |             0 |      5
       9 |     2 | jobman.examples.def_addition.addition_example | -2040929596667704625 |                   1 |             0 |      7
      10 |     2 | jobman.examples.def_addition.addition_example | -3750366477948382143 |                   1 |             0 |      9
      11 |     4 | jobman.examples.def_addition.addition_example |  2241733668528071327 |                   1 |             0 |      1
      12 |     4 | jobman.examples.def_addition.addition_example |  -267140279466343315 |                   1 |             0 |      3
      13 |     4 | jobman.examples.def_addition.addition_example | -6865789780959143221 |                   1 |             0 |      5
      14 |     4 | jobman.examples.def_addition.addition_example | -2040929596673704583 |                   1 |             0 |      7
      15 |     4 | jobman.examples.def_addition.addition_example | -3750366477942382121 |                   1 |             0 |      9
      16 |     6 | jobman.examples.def_addition.addition_example |  2241733668526071317 |                   1 |             0 |      1
      17 |     6 | jobman.examples.def_addition.addition_example |  -267140279464343305 |                   1 |             0 |      3
      18 |     6 | jobman.examples.def_addition.addition_example | -6865789780961143231 |                   1 |             0 |      5
      19 |     6 | jobman.examples.def_addition.addition_example | -2040929596671704637 |                   1 |             0 |      7
      20 |     6 | jobman.examples.def_addition.addition_example | -3750366477944382131 |                   1 |             0 |      9
      21 |     8 | jobman.examples.def_addition.addition_example |  2241733668516071355 |                   1 |             0 |      1
      22 |     8 | jobman.examples.def_addition.addition_example |  -267140279462343303 |                   1 |             0 |      3
      23 |     8 | jobman.examples.def_addition.addition_example | -6865789780947143121 |                   1 |             0 |      5
      24 |     8 | jobman.examples.def_addition.addition_example | -2040929596677704595 |                   1 |             0 |      7
      25 |     8 | jobman.examples.def_addition.addition_example | -3750366477938382045 |                   1 |             0 |      9
      26 |    10 | jobman.examples.def_addition.addition_example |  -179833476364920441 |                   1 |             0 |      1
      27 |    10 | jobman.examples.def_addition.addition_example |  4666783280000472973 |                   1 |             0 |      3
      28 |    10 | jobman.examples.def_addition.addition_example | -6021067085825160933 |                   1 |             0 |      5
      29 |    10 | jobman.examples.def_addition.addition_example | -6401888343550871263 |                   1 |             0 |      7
      30 |    10 | jobman.examples.def_addition.addition_example | -7084909074444200609 |                   1 |             0 |      9
     (30 rows)

Of course, you can select the columns you want to see, rename and
reorder them. For instance, you don't really care of ``jobman_hash``,
which is an internal field, or ``jobman_experiment``, since it is the
same for all experiments.

.. code-block::

    <database>=> SELECT id, jobman_status AS status, jobman_sql_priority AS priority, first, second FROM test_add_view;

     id | status | priority | first | second 
    ----+--------+----------+-------+--------
      1 |      0 |        1 |     0 |      1
      2 |      0 |        1 |     0 |      3
      3 |      0 |        1 |     0 |      5
      4 |      0 |        1 |     0 |      7
      5 |      0 |        1 |     0 |      9
      6 |      0 |        1 |     2 |      1
      7 |      0 |        1 |     2 |      3
      8 |      0 |        1 |     2 |      5
      9 |      0 |        1 |     2 |      7
     10 |      0 |        1 |     2 |      9
     11 |      0 |        1 |     4 |      1
     12 |      0 |        1 |     4 |      3
     13 |      0 |        1 |     4 |      5
     14 |      0 |        1 |     4 |      7
     15 |      0 |        1 |     4 |      9
     16 |      0 |        1 |     6 |      1
     17 |      0 |        1 |     6 |      3
     18 |      0 |        1 |     6 |      5
     19 |      0 |        1 |     6 |      7
     20 |      0 |        1 |     6 |      9
     21 |      0 |        1 |     8 |      1
     22 |      0 |        1 |     8 |      3
     23 |      0 |        1 |     8 |      5
     24 |      0 |        1 |     8 |      7
     25 |      0 |        1 |     8 |      9
     26 |      0 |        1 |    10 |      1
     27 |      0 |        1 |    10 |      3
     28 |      0 |        1 |    10 |      5
     29 |      0 |        1 |    10 |      7
     30 |      0 |        1 |    10 |      9
    (30 rows)

The ``priority`` decides the order in which the jobs will be executed, higher means first.

The ``status`` is the execution status. 0 means ready to execute, 1
means that the execution has started, and 2 that it's completed.

Executing the jobs
==================

Once the specifications of the job (the experiment function and its
arguments) are inserted into the database, they can be retrieved and
executed on any machine with access to this database.

The files that will be produced during the execution will be placed in a
unique subdirectory of the experiment root path, provided when launching
the job. For instance, if you want the experiment root to be the current
directory:

.. code-block::

    jobman sql postgres://<user>:<pass>@<server>/<database>/<table> .

You can also specify a distant path, if you want to gather results from
jobs executed on different machines:

.. code-block::

    jobman sql postgres://<user>:<pass>@<server>/<database>/<table> ssh://<fileserver>:<some>/<path>

The above commands will retrieve one job description among those with
highest priority, and that have not been started yet, and execute
it. You can also ask for several jobs to be executed one after the
other:

.. code-block::

    jobman sql -n3 postgres://<user>:<pass>@<server>/<database>/<table> .

will execute 3 jobs, and

.. code-block::

    jobman sql -n0 postgres://<user>:<pass>@<server>/<database>/<table> .

will keep on executing new jobs until all jobs are executed. You can
launch this command on different computers, or several times on a
cluster, to have jobs executed in parallel.

For more information:

.. code-block::

    jobman sql help


Querying the results
====================

Once the first job has finished execution, new keys are added to its
``state``. To account for them, you should recreate the view, by running
the code above (TODO: put reference).

Three fields have been added: ``jobman_sql_hostname`` and
``jobman_sql_hostworkdir``, which contain the hostname and temporary
working directory the job has been executed on, and ``result``, as
created by the experiment function (``addition_example``).

We can then use SQL syntax to retrieve the results of finished jobs:

.. code-block::

    <database>=> SELECT id, jobman_status AS status, jobman_sql_priority AS priority, first, second, result FROM test_add_view WHERE jobman_status = 2;
    id | status | priority | first | second | result 
   ----+--------+----------+-------+--------+--------
     1 |      2 |        1 |     0 |      1 |      1
   (1 row)

When several jobs are complete, you can filter and order the results:

.. code-block::

    <database>=> SELECT id, jobman_status AS status, jobman_sql_priority AS priority, first, second, result FROM test_add_view WHERE first > 4 AND second < 7 AND jobman_status = 2 ORDER BY result;

     id | status | priority | first | second | result 
    ----+--------+----------+-------+--------+--------
     16 |      2 |        1 |     6 |      1 |      7
     21 |      2 |        1 |     8 |      1 |      9
     17 |      2 |        1 |     6 |      3 |      9
     22 |      2 |        1 |     8 |      3 |     11
     18 |      2 |        1 |     6 |      5 |     11
     26 |      2 |        1 |    10 |      1 |     11
     27 |      2 |        1 |    10 |      3 |     13
     23 |      2 |        1 |     8 |      5 |     13
     28 |      2 |        1 |    10 |      5 |     15
    (9 rows)




========
 Jobman
========
--------------------------------
 A job manager to rule them all
--------------------------------

Intro
======

Jobman is for running Python functions, of the form

.. code-block:: python

    def <function>(state, channel):
       ...

Jobman takes care of filling the attributes of the ``state`` variable from a file, or the
commandline, or from a database.
If the function adds or changes attributes of the state, Jobman can save these attributes
automatically.  This is provided as a natural way to save the results of the function.

To run an experiment function, you must put that function in a module that can be 'import'ed.







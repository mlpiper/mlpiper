:orphan:

.. _introduction:

Introduction
--------------

The mlops python library provides an API for user code to communicate with MLOps. The mlops module
provides both an API to export statistics and ML objects from the user code to MLOps and an API to import
statistics and ML objects from MLOps to the user code.

Here is a simple example of how the mlops module can be used::

    >>> from parallelm.mlops import mlops
    >>> from parallelm.mlops import StatCategory as st
    >>> mlops.init(spark_context)
    >>> mlops.set_stat("my_value", 5.5)
    >>> mlops.done()

The above example does the following:

* Imports the mlops module (both mlops and StatCategory)
* Initializes the mlops library (calling mlops.init(spark_context))
* Reports one statistic called "my_value" from Spark into MLOps
* Closes the mlops module (just before stopping the Spark context)

The mlops module allows reporting various data types as statistics and also provides several categories of statistics.
The data types supported are Double, String, Vector, and RDD. The StatCategory class provides users the
ability to indicate what to do with the value provided. For example, StatCategory.CONFIG should be
used for values for which only the last value is required to be reported, while StatCategory.TIME_SERIES should be used
when all the values reported should be accumulated and displayed by time.

Providing Statistics with a Visual Representation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Statistics reported by the mlops module are delivered to MLOps and stored in a database.
The MLOps UI allows the user to view the statistics generated graphically. For example, a collection
of data points over time will be displayed as a line graph. In another example, the user of mlops can report a table
object, which will be displayed as a table in the UI.

Importing Statistics from MLOps to the User Program
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
All statistics reported by the application's different nodes are saved in the MLOps database.
Such statistics can be imported from the database by user code. The following example demonstrates how to do this.

    >>> mlops.init(spark_context)
    >>> now = datetime.now()
    >>> an_hour_ago = (now - timedelta(hours=1))
    >>> data_frame = mlops.get_stat("my_value", "0", start_time=an_hour_ago, end_time=now)
    >>> mlops.done()

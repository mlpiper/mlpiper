:orphan:

.. _calling_from_r:

##############################
Calling MLOps API from R code
##############################


Introduction
------------------------------

The MLOps API can be used in an R program. To do so, you must first import the "reticulate" library.
The "reticulate" library provides interoperability between R and Python code. We recommend that you read the
"reticulate" documentation prior to using the MLOps API in R in order to understand how R data structures are translated
into Python data structures.

The following code snippet shows how to use the MLOps API from R code:

    >>> library("reticulate")
    >>> mlops <- import("parallelm.mlops", convert = TRUE)
    >>> mlops <- mlops$mlops
    >>> mlops$init()
    >>> mlops$set_stat("stat-in-r-example", 1)

The first line imports the "reticulate" library. The library should be pre-installed in your R environment.
The second line imports the "parallelm.mlops" module.

The "convert" argument to the import function tells the "reticulate"
library to convert arguments from R representation to Python representation. This way, you do not need to convert
data types between the two languages.

The third line creates a handle to the mlops object from
the mlops module. This object is used to get and set data from the module.
The forth line initializes the mlops object by calling its init() method. The fifth line calls the set_stat
method of this object, reporting a statistic named "stat-in-r-example" with a value of 1.

In general, the MLOps API is used in R in the same way it is used in Python except that the "." is replaced with
"$".


Passing a Dataframe to MLOps APIs
---------------------------------

The following example shows how a dataframe (representing input data) is passed to the MLOps API:

    >>> test = data.frame(temperature_1 = rnorm(100),
    >>>              temperature_2 = rnorm(100),
    >>>              pressure_1 = sample.int(10, 100, replace = TRUE),
    >>>              pressure_2 = rep(c("A"    , "B", "C", "D"), 5))

    >>> mlops$set_data_distribution_stat(data=test)

The first line generates an R dataframe. Since the reticulate library automatically converts an R dataframe to a pandas
dataframe, calling the set_data_distribution method of the mlops object is straightforward.


Using MLOps Objects to Report Statistics
----------------------------------------

MLOps provides several objects for reporting different kinds of statistics, such as Table, MultiGraph, and others.
To use such objects in R, the objects need to be imported. This is similar to Python but the syntax is
a bit different.

The following example shows how to create a Table object, populate the table with data and send the table to MCenter.

    >>> mlops_tbl <- import("parallelm.mlops.stats.table", convert = TRUE)
    >>> mlops_tbl <- mlops_tbl$Table
    >>> tbl = mlops_tbl()$name("My Table")$cols(c("col 1 name", "col 2 name"))
    >>> tbl$add_row(c("v1", "v2"))
    >>> tbl$add_row(c("v3", "v4"))
    >>> mlops$set_stat(tbl)

The first 2 lines import the Table class.
The third line creates a table object and sets the
name of the table and the names of the columns. Note that in this line, an R list is provided;
since the convert=TRUE is used, this R list is converted to a Python list.
The 4th and 5th lines add 2 rows with data, and finally the 6th line reports the table to MCenter.

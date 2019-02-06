:orphan:

.. _Running_without_mlops:

#######################
Running Without MCenter
#######################


For ease of development and testing, code instrumented with the mlops module can also run without the MLOps product
infrastructure. In this case, statistics generated will be redirected to a file. But queries for historical statistics
will fail due to the missing MCenter environment.

The mlops python code is contained in the *parallelm-1.0.0-py2.7.egg* file which can be found inside
the MLOps tarball under the *pm-algorithms* directory. An egg for python 3.4 can also be found
in this directory. If running using Docker containers, the eggs are found in the directory
/opt/parallelm/mcenter/pm-algorithms/ inside the MCenter agent Docker image.

Before using the package, it is required to install few prerequisites. The following is a list of the MLOps package
prerequisites:

* Pandas - Python Data Analysis Libraray. Install using: ``python -m pip install pandas``
* Requests - HTTP for Humans™. Install using ``python -m pip install requests``


MLOps output redirection
---------------------------
By default, when running without MLOps, mlops redirects output to stdout. In order to redirect
mlops output to a file, the environment variable *PM_MLOPS_FILE* should be set to the path of the output file.


Example using spark-submit to run Spark code
---------------------------------------------
In the example below we will assume that the egg file is located here:
*/opt/parallelm/mlops/pm-algorithms/parallelm-1.0.0-py2.7.egg*


.. code-block:: bash

 env PM_MLOPS_FILE="/tmp/mlops.csv" \
    spark-submit
    --master spark://localhost:7077 \
    --py-files /opt/parallelm/mlops/pm-algorithms/parallelm-1.0.0-py2.7.egg \
    pi_calc.py


This example uses the environment variable *PM_MLOPS_FILE* to redirect the output of mlops to the
*/tmp/mlops.csv* file


Example using Python interpreter to run stand alone Python code
---------------------------------------------------------------
In the example below we assume the egg file is located here:
*/opt/parallelm/mlops/pm-algorithms/parallelm-1.0.0-py2.7.egg*


.. code-block:: bash

 env PYTHONPATH=/opt/parallelm/mlops/pm-algorithms/parallelm-1.0.0-py2.7.egg \
     PM_MLOPS_FILE="/tmp/mlops.csv" \
     python my_python_code.py


This example uses the environment variable *PYTHONPATH* to point to the MLOps egg and the *PM_MLOPS_FILE* to redirect
mlops output to the file */tmp/mlops.csv*. The code in the file *my_python_code.py* will be able to import
the mlops library and call its methods.


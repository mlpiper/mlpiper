# README

The `mlcomp` module is designed to process and run complex PySpark pipelines, which contain
multiple PySpark components. These components can be uploaded by the user.

## How to build and upload a component

#### Steps

- Create a folder, whose name corresponds to the component's name (.e.g pi_calc)

- Create a `component.json` file (json format) inside this folder and make sure to fill in all the following
  fields:

        {
            "engineType": "PySpark",
            "language": "Python",
            "userStandalone": false,
            "name": "<Component name (.e.g pi_calc)>",
            "label": "<A lable that is displayed in the UI>",
            "version": "<Component's version (e.g. 1.0.0)>",
            "group": "<One of the valid groups (.e.g "Algorithms")>,
            "program": "<The Python component main script (.e.g pi_calc.py)>",
            "componentClass": "<The component class name (.e.g PiCalc)>",
            "useMLStats": <true|false - whether the components uses mlstats>,
            "inputInfo": [
                {
                 "description": "<Description>",
                 "label": "<Lable name>",
                 "defaultComponent": "",
                 "type": "<A type used to verify matching connected legs (e.g 'org.apache.spark.rdd.RDD[int]')>,
                 "group": "<data|model|prediction|statistics|other>"
                },
                {...}
            ],
            "outputInfo": [
                <Same as inputInfo above>
            ],
            "arguments": [
                {
                    "key": "<Unique argument key name>",
                    "type": "int|long|float|str|bool",
                    "label": "<A lable that is displayed in the UI>",
                    "description": "<Description>",
                    "optional": <true|false>
                }
            ]
        }

- Create the main component script, which contains the component's class name. This class
  should inherit from a 'Component' base class, which is taken from `parallelm.components.component`.
  The class must implement the `materialize` function, with this prototype:
  `def materialize(self, sc, parents_rdds)`. Here is a complete self contained example:

        import numpy as np
        from parallelm.components.component import Component


        class NumGen(Component):
            num_samples = 0

            def __init__(self):
                super(self.__class__, self).__init__()

            def materialize(self, sc, parents_rdds):
                num_samples = self._params['num_samples']
                self._logger.info("Num samples: {}".format(num_samples))

                rdd = sc.parallelize([0] * num_samples).map(NumGen._rand_num)
                return [rdd]

            @staticmethod
            def _rand_num(x):
                return (np.random.random(), np.random.random())

  Notes:
    - `num_samples` is an argument to the given component and thus can be read from `self._params`.
    - A component can use `self._logger` object to print logs. It is defined in the base `Component` class.
    - In this case the component uses the `numpy` module.
    - A static function can be used in the `map` api of an `RDD` (.e.g `NumGen._rand_num`).

- Place the components main program (*.py) inside that folder along with any other
  desired files.

- Pack the folder, using the `tar` tool. The extension should be `.tar`:

        > tar cf pi_calc.tar ./pi_calc

- Use the MLOps center UI to upload the component.


**Note:** Complete example components can be found under `./test/comp-to-upload/parallelm/uploaded_components/`


## Tools

Handy tools are located under `./bin` folder. These tools are used by the build system
as well as the testing tools.


### create-egg.sh

Builds and generates new `egg` distribution. The result is placed under `./dist` folder.


### cleanup.sh

Cleanups all generated products created by the `create-egg.sh`


## Testing

The whole module can be tested by running the `./test/run-test.sh` tool. This tool
can be run in either local or external (default) modes.

* Local mode (`--run-locally`) - Run Spark locally with as many worker threads as logical cores on your machine.
  The local mode means that the Spark is embedded withing the driver itself and does not
  require an external standalone Spark cluster.
* External mode (**default**) - Submit the application to a standalone cluster that run on the same
  machine. It is required to run the spark cluster on the `localhost` interface (It can be
  achieved by setting an env variable as follows: `SPARK_MASTER_HOST=localhost`).
  <b>Note: You may also run `<reflex-root>/tools/local-dev/start-externals.sh`, which runs
  the Spark cluster as necessary.


### Example Components

Example components are located under: `./test/comp-to-upload/parallelm/uploaded_components`.


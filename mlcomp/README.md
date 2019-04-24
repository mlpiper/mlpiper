# README

The `mlcomp` module is designed to process and execute complex pipelines,that consist of one or more components chained together such that output of a previous component becomes the input to the next component. Each pipeline has a
particular purpose, such as to train a model or generate inferences.

A single pipeline may include components from different languages, such as Python, R and Java.

## Quickstart

#### Steps

- Create a pipeline. Open any text editor and copy the following pipeline description:

        {
            "name": "Simple MCenter runner test",
            "engineType": "Generic",
            "pipe": [
                {
                    "name": "Source String",
                    "id": 1,
                    "type": "string-source",
                    "parents": [],
                    "arguments": {
                        "value": "Hello World: testing string source and sink"
                    }
                },
                {
                    "name": "Sink String",
                    "id": 2,
                    "type": "string-sink",
                    "parents": [{"parent": 1, "output": 0}],
                    "arguments": {
                        "expected-value": "Hello World: testing string source and sink"
                    }
                }
            ]
        }

- Clone `mlpiper` repo [https://github.com/mlpiper/mlpiper/](https://github.com/mlpiper/mlpiper/)
- Components `string-source` and `string-sink` can be found in the repo path [https://github.com/mlpiper/mlpiper/tree/master/reflex-algos/components/Python](https://github.com/mlpiper/mlpiper/tree/master/reflex-algos/components/Python)
- Once the `ml-comp` python package is installed, the `mlpiper` command line tool is available and can be used to execute the pipeline above and the components described in it. Run the example above with:

      mlpiper run -f ~/<pipeline description file> -r <path to mlpiper repo>/reflex-algos/components/Python/ -d <deployment dir>

     Use the **--force** option to overwrite the deployment directory.

## How to construct a component

#### Steps

- Create a directory, the name of which corresponds to the component's name (e.g., source_string)

- Create a `component.json` file (JSON format) inside this directory and make sure to fill in all of the following fields:

        {
            "engineType": "Generic",
            "language": "Python",
            "userStandalone": false,
            "name": "<Component name (e.g., string_source)>",
            "label": "<A lable that is displayed in the UI>",
            "version": "<Component's version (e.g., 1.0.0)>",
            "group": "<One of the valid groups (e.g., "Connectors")>,
            "program": "<The Python component main script (e.g., string_source.py)>",
            "componentClass": "<The component class name (e.g., StringSource)
            "useMLStats": <true|false - (whether the components uses MLStats)>,
            "inputInfo": [
                {
                 "description": "<Description>",
                 "label": "<Lable name>",
                 "defaultComponent": "",
                 "type": "<A type used to verify matching connected legs>,
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
                    "label": "<A label that is displayed in the UI>",
                    "description": "<Description>",
                    "optional": <true|false>
                }
            ]
        }

- Create the main component script, which contains the component's class name.
  This class should inherit from a 'Component' base class, which is taken from
  `parallelm.components.component`. The class must implement the `materialize`
  function, with this prototype: `def _materialize(self, parent_data_objs, user_data)`.
  Here is a complete self contained example:

        from parallelm.components import ConnectableComponent
        from parallelm.mlops import mlops


        class StringSource(ConnectableComponent):
            def __init__(self, engine):
                super(self.__class__, self).__init__(engine)

            def _materialize(self, parent_data_objs, user_data):
                self._logger.info("Inside string source component")
                str_value = self._params.get('value', "default-string-value")

                mlops.set_stat("Specific stat title", 1.0)
                mlops.set_stat("Specific stat title", 2.0)

                return [str_value]


  Notes:
    - A component can use `self._logger` object to print logs.
    - A component may access to pipeline parameters via `self._params` dictionary.
    - The `_materialize` function should return a list of objects or None otherwise.
      This returned value will be used as an input for the next component
      in the pipeline chain.

- Place the component's main program (\*.py) inside a directory along with its JSON
  description file and any other desired files.


## How to construct a pipeline

#### Steps

- Open any text editor and copy the following pipeline description:

        {
            "name": "Simple MCenter runner test",
            "engineType": "Generic",
            "pipe": [
                {
                    "name": "Source String",
                    "id": 1,
                    "type": "string-source",
                    "parents": [],
                    "arguments": {
                        "value": "Hello World: testing string source and sink"
                    }
                },
                {
                    "name": "Sink String",
                    "id": 2,
                    "type": "string-sink",
                    "parents": [{"parent": 1, "output": 0}],
                    "arguments": {
                        "expected-value": "Hello World: testing string source and sink"
                    }
                }
            ]
        }

  Notes:
    - It is assumed that you've already constructed two components whose names
      are: `string-source` and `string-sink`
    - The output of the `string-source` component (the value returned from
      `_materialize` function) is supposed to become the input of the `string-sink`
      component (an input to the `_materialize` function)
 
- Save it with any desired name


## How to test

Once the `ml-comp` python package is installed, `mlpiper` command line tool is available
and can be used to execute the pipeline above and the components described in it.

There are three main commands that can be used as follows:

  - **deploy** - Deploys a pipeline along with provided components into a given
                 directory. Once deployed, it can be executed directly from 
                 the given directory.

  - **run** - Deploys and then executes the pipeline.

  - **run-deployment** - Executes an already-deployed pipeline.


#### Examples:

  - Prepare a deployment. The resulting directory will be copied to a docker container and run
    there:

        mlpiper deploy -f p1.json -r ~/dev/components -d /tmp/pp

  - Deploy & Run. Useful for development and debugging:

        mlpiper run -f p1.json -r ~/dev/components -d /tmp/pp

       Use **--force** option to overwrite deployment directory

  - Run a deployment. Usually non-interactive and called by another script:

        mlpiper run-deployment --deployment-dir /tmp/pp

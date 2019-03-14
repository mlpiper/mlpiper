# README

The `mlcomp` module is designed to process and execute complex pipelines,
which consists of one or more component chained together such that output of a
previous component becomes the input to the next component. Each pipeline has a
particular purpose, such as to train a model or generate inferences.

A single pipeline may include component from different languages, such as Python,
R and Java.

## How to construct a component

#### Steps

- Create a folder, whose name corresponds to the component's name (.e.g source_string)

- Create a `component.json` file (json format) inside this folder and make sure to
  fill in all the following fields:

        {
            "engineType": "Generic",
            "language": "Python",
            "userStandalone": false,
            "name": "<Component name (.e.g string_source)>",
            "label": "<A lable that is displayed in the UI>",
            "version": "<Component's version (e.g. 1.0.0)>",
            "group": "<One of the valid groups (.e.g "Connectors")>,
            "program": "<The Python component main script (.e.g string_source.py)>",
            "componentClass": "<The component class name (.e.g StringSource)
            "useMLStats": <true|false - whether the components uses mlstats>,
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
                    "label": "<A lable that is displayed in the UI>",
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

- Place the components main program (*.py) inside a folder along with its json
  description file and any other desired files.


## How to construct a pipeline

#### Steps

- Open any text editor and copy the following template:

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
    - The output of `string-source` component (the value returned from
      `_materialize` function) is supposed to become the input of `string-sink`
      component (an input to the `_materialize` function)
 
- Save it with any desired name


## How to test

Once the `ml-comp` python package is installed, a command line `mlpiper` is installed
and can be used to execute the pipeline above and the components described in it.

There are three main commands that can be used as follows:

  - **deploy** - deploys a pipeline along with provided components into a given
                 folder. Once deployed, it can also be executed directly from 
                 the given folder.

  - **run** - deploys and executes the pipeline at once.

  - **run-deployment** - executes an already deployed pipeline.


#### Examples:

  - Prepare a deployment. The resulted dir will be copied to a docker container and run
    there

        mlpiper deploy -p p1.json -r ~/dev/components -d /tmp/pp

  - Deploy & Run. Useful for development and debugging

        mlpiper run -p p1.json -r ~/dev/components -d /tmp/pp

  - Run a deployment. Usually non interactive called by another script

        mlpiper run-deployment --deployment-dir /tmp/pp

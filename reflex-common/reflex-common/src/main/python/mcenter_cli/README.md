# MCenter Command Line application

mcenter-cli is a command line program that provide an easy way to access MCenter and perform operations from the
command line.

## How to run

In order to run mcenter-cli, first save the mcenter-cli python wheel file
(e.g. mcenter_cli-1.0.0-py2-none-any.whl) on your local file system.

Then run:

      > python dist/mcenter_cli-1.0.0-py2-none-any.whl --help


# Structure of MLApp directory

An MLApp is defined by a directory structure containing several files and directories inside.

* mlapp.json
* mlapp_tmpl.json
* pipelines (directory

The main definition file is the mlapp.json file. This file contains the structure of the mlapp and some of the values
to use for its specific attributed.

Below is an example of such file:

{
       "name": "predict-mlapp",
       "pipelines": [
		{
                "workflow_ref_id": "1",
                "group": "group_spark",
		        "default_model_name": "model_test_1",
                "pipeline_pattern": "./pipelines/predict.json",
		        "profiles": [
                 {
                    "group": "group_spark",
                    "profile_path": "./pipelines/predict.json"
                 }]
          	}
        ]
}

This file defines mostly the pipeline parameters to use.


The second file is the mlapp_tmp.json:

{
    "workflow": [
         {
            "children": "-1",
            "parent": "-1",
            "groupId": "placeholder",
            "pipelineId": "placeholder",
            "pipelineType": "model_consumer",
            "id": "1",
            "pipelineMode": "offline",
            "cronSchedule":"* 0/2 * * * ?"
        }
    ],
    "modelPolicy": "always_update"
}

This file defines the nodes in the mlapp (the items of the "workflow" list)


The pipeline directory contains the pipeline files. Note that the mlapp.json refer to these files in the
pipeline_pattern property.


{
          "name" : "predict-example",
          "engineType": "Python",
          "pipe" : [
              {
                  "name": "predict-test",
                  "id": 1,
                  "type": "predict-test",
                  "parents": [],
                  "arguments" : {
                  }
              }
          ]
 }

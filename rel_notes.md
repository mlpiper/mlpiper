Release Notes & Change Logs
===========================

MCenter 1.3.0
-------------

***KI-0002***

MCenter 1.0 supports RSA SSH. Support for DSA SSH method will be added in a later release.

***KI-0003***

There is rare occurance of message loss if a message is sent during an MLApp failover that can affect MLApp execution.

***KI-0004***

Customers should be aware that the scheduling mechanisms in Spark can result in MLApps of certain configurations becoming unable to run. This only occurs if MLApps with interdependent pipelines are scheduled on a spark resource that cannot accommodate running all MLApp pipelines concurrently.
An example could be an inference pipeline (with no default model) that is scheduled to run before its dependent training pipeline where both pipelines compete for a single resource in Spark will result in the inference pipeline blocked waiting for the training pipeline, which never gets to run.

***KI-0007***

MLApp save operation does not support non-alphanumeric characters in MLApp definition.


***KI-0015***

If an MLApp is launched where a pipeline is missing an uploaded component, the MLApp will fail.

***KI-0018***

Configurable parallelism support for Spark (as part of the pipeline properties) is currently disabled. It will be added in the next release. "spark.deploy.defaultCores" variable can be updated in the spark-default configuration file to choose the number of cores to use for a given pipeline.

***KI-0020***

Information provided in Model Upload field is not available in subsequent Model Lineage views for the uploaded model.

***KI-0023***

*Always Reject* option in Model Update policy is disabled.

***KI-0025***

The *get_events* MLOps API does not return any events related to *Canary* alerts.

***KI-0026***

Spark PMML inference parameters are unused in the current release.

***KI-0027***

Model upload operation works only for a models uploaded as single files. Models structured as directories will be supported in future releases. For the current release, convert model directories into a single file (via utilities such as tar) and add matching unpack utilities in consuming pipelines.

***KI-0028***

MLApps can occasionally fail with error string *Error: com.google.gson.stream.MalformedjsonException*.

***KI-0029***

All times are local, except for logs, which are UTC.

***KI-0030***

Log messages may contain references to "MLOps". "MLOps" was the previous name of the MCenter product. These log messages are resolved in the next release of MCenter.

***KI-0031***

Support for categorical values is currently limited to features with less than 25 unique values. Features with more than 25 unique values are treated as numeric/continuous values.

***KI-0032***

In this release, it is not possible to remove an uploaded component.

***KI-0033***

Connectable components are supported for Built-in Spark (i.e., Spark Batch), Python, and R. Support for PySpark SparkML connectable components will be added in the upcoming release.

***KI-0034***

Health view events panel shows alerts in verbose (mode) for set thresholds, this issue will be resolved in the upcoming release.

***KI-0035***

In this release, AB Test and Canary requires groups to be configured in the MLApp pattern. If groups are not configured, the MLApp will fail on execution.

***KI-0036***

In this release, prediction distributions should be explicitly created by users within the inference pipeline and exported via MLOps APIs for the Canary Comparator to fetch the predictions.

***KI-0037***

In this release, model propagation for PySpark pipelines on YARN requires a scratch (or staging) HDFS directory to store and transfer models. Pipelines need to include a code snippet to save models to the scratch HDFS directory.

***KI-0038***

In this release, auxiliary pipelines besides health (Canary, or AB Testing) are not supported and executing MLApps with such pipelines will result in Health View to not load correctly.

***KI-0039***

The intersection of the A/B scores may not always be shown accurately for the AB test comparator graphs.

***KI-0040***

In the current release, ML Health view can occasionally fail to show the histograms for the Categorical data over time.

***KI-0041***

In the current release, the Configuration view incorrectly reports "SQLConnector" error for garbage collected MLApps. These errors can safely be ignored, the issue will be resolved in the upcoming release.

***KI-0042***

In health management, transitioning from per-attribute threshold to global threshold would report each attribute value to be changed individually. This message will be simplified in the next release.

***KI-0043***

In the current release, model unavailability needs to be handled gracefully in the model-consumer pipeline. Failure to do will result in the termination of the running MLApp Instance.

***KI-0044***

In rare instancesi, navigating from alert view modal for data-deviation alerts, to Health view using the hyperlink provided for the provided time-window can lead to the heatmap not being shown. This is a known issue and will be resolved in the next release. Workaround for the issue is to choose a larger time window on the Health view.

***KI-0045***

For the instances of Completed/Archived ML App with a deleted profile, the Configuration view will be disabled for the ML App.

***KI-0046***

Uploaded Spark ML components require a shared location to act as an intermediate staging area for models. For inference pipelines, the user program must copy the model from the MCenter-provided path to the shared location before loading. For training pipelines, the user program must save the model to the shared path then explicitly copy the model to the MCenter-provided model path. More details can be found in the section Manual Component Uploading. This behavior will be simplified in the upcoming release.

***KI-0047***

In rare instances, navigating from Configuration view to fetch ML App logs will result in error "Error while retrieving logs: End offset must be greater than start offset" being reported. The error can safely be ignored and will be resolved in the upcoming release.

Change Logs [v1.3.0] - 2018-12-10
---------------------------------

**New Features:**

- Governance now also provides Provenance details for active models.
- Enhanced Model comparison metrics, using model associated stats and respective source, approval status and usages.
- The TensorFlow engine has been renamed the Process engine since the Python engine now supports TensorFlow jobs as well.

**Improvements:**

- MCenter License Terms have been added.
- Model producing components can be tagged "explainable" (includes user uploaded components), the pipeline comprising of "explainable" components automatically get marked as explainable. MLApp built with "explainable" pipelines are automatically marked explainable.
- Added support to run TensorFlow MLApp in Docker Container.

**Bug Fixes:**

- Single dimension ndarray handling for histogram creation.
- Support for file-channel using Pandas Dataframe.

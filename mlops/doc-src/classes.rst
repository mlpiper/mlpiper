:orphan:

.. _classes:

Classes
-------

.. contents::

The mlops module provides classes to create and report statistics, events, models, KPI, and more.
This data can then be reported to MLOps or retrieved from MLOps from Spark and python
programs.


Event Types
===========

.. autoclass:: parallelm.mlops.events.event_type.EventType
    :members:
    :member-order: bysource

Statistics Objects
==================

The mlops module provides several classes which represent different statistics types supported by MLOps.
Each such object provides methods to set the attributes relevant to the given statistic type.
The mlops.set_stat call is used to submit such objects to MLOps.

.. autoclass:: parallelm.mlops.StatCategory
    :members:
    :member-order: bysource

.. autoclass:: parallelm.mlops.stats.table.Table
    :members: name, columns_names, cols, add_row
    :member-order: bysource

.. autoclass:: parallelm.mlops.stats.bar_graph.BarGraph
    :members: name, columns_names, cols, data
    :member-order: bysource

.. autoclass:: parallelm.mlops.stats.multi_line_graph.MultiLineGraph
    :members: name, labels, data
    :member-order: bysource

.. autoclass:: parallelm.mlops.stats.graph.Graph
    :members: name, x_title, y_title, set_categorical, set_continuous, set_x_series, add_y_series, annotate
    :member-order: bysource

.. autoclass:: parallelm.mlops.stats.graph.MultiGraph
    :members: name, x_title, y_title, set_categorical, set_continuous, add_series, annotate
    :member-order: bysource

.. autoclass:: parallelm.mlops.stats.opaque.Opaque
    :members: name, data
    :member-order: bysource

MLApp Objects
=============

.. autoclass:: parallelm.mlops.ion.ion.MLAppNode
    :members:
    :member-order: bysource

.. autoclass:: parallelm.mlops.ion.ion.Agent
    :members:
    :member-order: bysource

MLOps API
=========

This interface provides methods for creating, reporting, and retrieving statistics, events, models,
KPI, and other information about the ML runtime environment.

.. py:currentmodule:: parallelm.mlops

.. autoclass:: MLOps
    :members: init, done, get_mlapp_id, get_mlapp_name, get_mlapp_policy, get_nodes, get_node, get_current_node,
        get_agents, get_agent, current_model, set_stat, set_kpi, set_data_distribution_stat, get_stats, get_kpi,
        get_models_by_time, get_model_by_id, data_alert, health_alert, system_alert, canary_alert, set_event,
        get_events
    :member-order: bysource


MLOps Metrics API
=================

Class is responsible for giving user sklearn alike code representation for using ParallelM's mlops apis.
Class supports classification, regression and clustering stats.

.. autoclass:: parallelm.mlops.mlops_metrics.MLOpsMetrics
    :members:
    :member-order: bysource

.. autoclass:: parallelm.mlops.mlops_metrics.MLOpsClusterMetrics
    :members:
    :member-order: bysource

Exceptions
==========

The mlops library raises exceptions of type ``MLOpsException(Exception)``; The mlops.metrics library raises exceptions of type ``MLOpsStatisticsException(Exception)``


Spark Pipeline Model Helper
===========================

.. autoclass:: parallelm.mlops.common.spark_pipeline_model_helper.SparkPipelineModelHelper
    :members:
    :member-order: bysource

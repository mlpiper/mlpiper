# Quick Start

This section will help you create and manage your first MLApp! Please complete the
installation of the MCenter server and agent before you proceed with the quick
start guide.  

** Onboarding **
---------------

The *Onboarding* Tab helps users create or import existing MLApps into MCenter.  

### Registering Agents

Agents need to configured to use a resource manager (such as standalone Spark,
YARN, Docker Server, etc.). The admin needs to register the agent with the
MCenter server to execute ML pipelines.

To register an agent, a) login as an admin, b) click the *Settings* Icon ->
Agents, c) click *Register Agent* button, d) fill in the host or ip address of
the agent, and e) click *Register*. 

### Creating Execution Environments

An execution environment is a collection of configuration information used to
run pipeline on a resource manager, such as Spark or Kubernetes. Each execution
environment is configured on one agent that serves as the connection from 
MCenter server and the resource manager. To run a pipeline
on an engine, you first need to configure it by creating an execution environment.

To create an execution environment, a) click *Settings* and then *Execution Environments*,
b) click *Execution Environment* button, c) fill in the name, d) select the agent,
e) select the resource manager type, f) fill in the configuration, and g) click *Create*.    
  
### Creating Components

A component is a program or unit of processing. Components are the building
blocks for pipelines.

### Creating Pipelines

A pipeline consists of one or more components chained together such that the output
of a previous component becomes the input to the next component. Each pipeline
has a particular purpose, such as to train a model or generate inferences. Each
pipeline runs on a particular engine type (e.g., Spark, Docker).

### Creating MLApps

An MLApp consists of one or more pipelines. The MLApp contains information
about how its pipelines are orchestrated with respect to one another. For
example, the model produced by one pipeline may be consumed by another
pipeline. More generally, an MLApp represents the relationship between its
constituent pipelines as a directed graph, where each node is a pipeline and
the edges between nodes signify communication between the pipelines. In this
example, the communicated message is a trained model.

Although each pipeline can only run on one engine type, MLApps can contain
pipelines that run on different engine types. MLApps have many facets that
represent different aspects of a business application.

MCenter supports Patterns and Profiles for all MLApps. Patterns help capture
the structure (such as pipelines) and dependencies within an MLApp. Through
patterns, an MLApp can be leveraged for multiple use cases. Profiles
provide the ability to modify pipeline parameters, configuration (such as
schedule) and policies without having to recreate a new MLApp.

Pipelines are required to create an MLApp. The binding of pipelines to an
execution environment is done via Nodes. A Node encapsulates the ML Pipeline
and also the agent that the pipeline needs to execute on.
Patterns are the basic building block of an MLApp. 

To create a new MLApp: a) click *Create Pattern* button on the top right; b)
enter the name of your MLApp and c) click *Create* button.

An empty canvas is created for your new MLApp. Click on the empty node (i.e.,
Node 1) to add a pipeline, execution environment, schedule, and properties.

To configure a Node: a) select the execution environment, b) select the
pipeline from the pipelines tab. Leave the schedule and properties to be empty.
They can be configured when creating a profile. 

Repeat the above step to create additional nodes (if needed). 

**Important Considerations:**

1. An MLApp should contain at least one node with a pipeline in it.

2. To save the MLApp, each node in the MLApp should have a pipeline attached to it.

3. A default schedule can be attached to each individual node if desired. The
pipeline attached to the node will run on the pre-defined schedule. The
schedule for the individual nodes can be overwritten when creating a profile.  

4. A node with a Model Producer (i.e., (re)training) pipeline can be connected
to a Node with a Model Consumer (i.e., testing/scoring/inference) pipeline to
facilitate the transfer of the newly trained model by the MCenter server and
agent(s). Without this connection, transfer of models would be the
responsibility of the user.

### Configuring MLApps

Profiles need to be created to launch (or execute) an MLApp. Profiles are
created using MLApp pattern objects. In other words, profiles help users create
custom configurations on top of existing MLApp pattern objects.  

Profiles require a pattern object to be created first. Profile objects can be
deployed in production or sandbox mode via the MCenter server.

To create a new MLApp profile: a) click *Onboarding* b) click *Profiles*, c) click
*Create Profile* button on the top right, d) fill in the name of the MLApp
profile, e) click *Create* button. 

A wizard is provided to guide users through profile configuration. One can navigate
through the configuration process by simply clicking on the *Next* button on
the bottom right until the *Finish and Save* button appears. Clicking on the
*Finish and Save* button completes the creation of the profile.

**Important Considerations:**

1. Attaching an execution environment to a node is mandatory before launching a profile.
This selects where each pipeline will run.

2. MLApps are executed only on a specified schedule (in production and sandbox
mode).  Models are produced only after the *Model Producer* (i.e.,
(re)training) pipeline is run for the first time. 

3. If the (re)training pipelines are going to run very infrequently, we
strongly suggest to set a default model for the *Model Consumer* (i.e.,
scoring)  pipeline to continue scoring until a new model is generated by the
Model Producer pipeline. 

4. If Manual approval policy is selected, Models are not automatically
propagated to the linked Model Consumer pipelines until they are reviewed and
approved by a user with appropriate priveledges.

5. MLApps with only Model Consumer pipeline(s) require default models to be set
to enable scoring. Without a default model, these pipelines won’t be able to
score data samples until a model is set during the MLApp’s runtime on the
Dashboard page.

6. Failure of a pipeline within the MLApp results in termination of the MLApp.
  
### Launching MLApps

MLApps can be launched (or executed) in either sandbox or production mode.

MCenter provides production-grade sandbox environments for a range of testing
scenarios from simple developer-oriented tests to large, production-scale
tests. 

To launch an MLApp in sandbox mode: a) Click the *Actions* button on the
desired MLApp profile, b) Select *Launch in Sandbox* from the drop-down menu,
c) Select *Run Now* scheduler option, d) fill in the desired number of
iterations, and e) click the *Launch* button.  

When an MLApp is launched in production mode, all events, alerts, and
notifications occur in the production sections of the MCenter UI.

To launch an MLApp in production mode: a) Click the *Actions* button on the
desired MLApp profile, b) Select *Launch in Production* from the drop-down
menu, c) Select the *service account* from the drop-down, and d) click the
*Launch* button.  

**Important Considerations:**

1. Launching an MLApp does not execute the attached ML pipelines immediately.
ML pipelines are only executed on the pre-defined schedules unless the MLApp is
launched in Sandbox mode with the *Run Now* scheduler option.

2. The schedule of pipeline execution follows the `cron` scheduler behavior.

# Managing MLApps

ParallelM MCenter provides a comprehensive set of tools to monitor, diagnose,
and improve the functioning of ML pipelines. These include dashboards, alerts,
diagnostic recordings, and tools to detect unexpected behavior from ML models.
All diagnostic components, statistics, events, and alerts are available both as
built-in functions and as interfaces to enable custom development.

To manage MLApps: click *Dashboard* on the top left. 

#! /usr/bin/python3

from __future__ import print_function
import time

import sys
import traceback

sys.path.append('..')

import json

import parallelm.mcenter_client as mcenter_client

client = mcenter_client.Client(server='bran-c14:3456',
                               user=dict(username='admin', password='admin'))

r = client._rest.default.settings_global_parameters_get()
#r = client._rest.default.settings_build_manifest_get()
print("Type=%s, Val=%s, R=%r" % (type(r), r, r))
raise ValueError("Done here")

first_group = client.group.list()[0]
sample_agent = first_group.agents[0]
group = client.group.create(dict(name='testgroup', agents=[sample_agent]))
print("Got group %s" % group)
print("Deleting group")
group.delete()
print("Done!")


me = client.user.me()
print("I am: %s" % me)
print("My name is: %s" % me.name)
for u in client.user.list():
    if u.username == 'testuser':
        u.delete()
clone = me.data
clone.username = 'testuser'
clone.password = 'pwd'
new_user = client.user.create(clone)
print("New user is: %s" % new_user)
new_user.update(password='temporary', created='lisa')
with new_user as x:
    x.password = 'test'
    x.created = 'lisa'
    x.groups = []
new_user.password = 'abc'
print("Deleting user")
new_user.delete()


client._client = None
client = None
clone = None
new_user = None

sys.exit(0)

import mcenter_rest_api
from mcenter_rest_api.rest import ApiException

# Configure API key authorization: apiAuth
configuration = mcenter_rest_api.Configuration()
configuration.api_key['token'] = 'YOUR_API_KEY'
# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['token'] = 'Bearer'

api_client = mcenter_rest_api.ApiClient(configuration)

# create an instance of the API class
api_instance = mcenter_rest_api.DefaultApi(api_client)
user_instance = mcenter_rest_api.UsersApi(api_client)
group_instance = mcenter_rest_api.GroupsApi(api_client)
agent_instance = mcenter_rest_api.AgentsApi(api_client)
pipeline_instance = mcenter_rest_api.PipelineProfilesApi(api_client)
ppatterns_instance = mcenter_rest_api.PipelinePatternsApi(api_client)
try:
    # Create new agent
    res = user_instance.auth_login_post(dict(username='admin',
                                             password='admin',
                                             authMode='internal'))
    print("RES=%r, %r\n" % (res, type(res)))
    token = res.token
    api_client.cookie = 'token=' + token
    if False:  # tested with v2
        res = user_instance.auth_validate_post("Bearer " + token)
        print("Validate=%r\n" % res)
        res = user_instance.me_get()
        print("Me=%r\n" % res)
        res = user_instance.users_get()
        print("Users=%r\n" % res)
        for user in res:
            if user.username in ['testuser', 'testuser2']:
                dres = user_instance.users_user_id_delete(user.id)
                print("DeleteUser=%r\n" % dres)
        new_user = res[0]
        new_user.username = 'testuser'
        new_user.password = 'testpassword'
        res = user_instance.users_post(new_user)
        print("CreateUser=%r\n" % res)
        new_user = res
        new_user.username = 'testuser2'
        res = user_instance.users_user_id_put(new_user.id, new_user)
        print("UpdateUser=%r\n" % res)
        res = user_instance.users_user_id_get(new_user.id)
        print("GetUser=%r\n" % res)
        res = user_instance.users_user_id_delete(new_user.id)
        print("DeleteUser=%r\n" % res)
    if True:
        res = group_instance.onboarding_groups_get()
        print("Groups=%r\n" % res)
        for group in res:
            if group.name in ['test group', 'test group 1']:
                r2 = group_instance.onboarding_groups_group_id_delete(group.id)
                print("DeletedGroup=%r\n" % r2)

    # Delete test pipeline
    if True:
        res = pipeline_instance.onboarding_pipeline_profiles_get()
        print("Pipelines=%r\n" % res)
        for pipeline in res:
            if pipeline.name in ['test pipeline', 'test pipeline 1']:
                print("Pipeline id is %s\n" % pipeline.id)
                r2 = api_instance.pipelines_id_delete(pipeline.id)
                print("DeletedPipeline=%r\n" % r2)
    if True:
        res = api_instance.ml_app_instances_get()
        print("workflows=%r\n" % res)
        for workflow in res:
            if workflow.name in ['test workflow', 'test workflow 1']:
                print("Workflow id is %s\n" % workflow.id)
                r2 = api_instance.workflows_id_delete(workflow.id)
                print("Deletedworkflow=%r\n" % r2)
    if True:
        res = agent_instance.agents_get()
        print("Agents=%r\n" % res)
        for x in res:
            if x.address in ['1.1.1.1', 'localhost']:
                r2 = agent_instance.agents_agent_id_delete(x.id)
                print("DeleteAgent=%r\n" % r2)
        res = agent_instance.agents_post(dict(address='1.1.1.1'))
        print("CreateAgent=%r\n" % res)
        res = agent_instance.agents_agent_id_put(res.id,
                                                 dict(address='localhost',
                                                      name='test agent'))
        print("PutAgent=%r\n" % res)
        res = agent_instance.agents_agent_id_get(res.id)
        print("AgentId get=%r\n" % res)
    if True:
        agent_id = res.id
        res = group_instance.onboarding_groups_post(dict(name='test group',
                                                         agents=[agent_id]))
        print("CreateGroup=%r\n" % res)
        res.name = "test group 1"
        res = group_instance.onboarding_groups_group_id_put(res.id, res)
        print("UpdateGroup=%r\n" % res)
        res = group_instance.onboarding_groups_group_id_get(res.id)
        print("Group_id_get group=%r\n" % res)
    if True:
        res = ppatterns_instance.onboarding_pipeline_patterns_get()
        print ("Pipelines list:%r\n" % res)
        sample_pipeline = res[0]
    if False:
        sample_pipeline.name = 'test pipeline'
        res = api_instance.pipelines_post(sample_pipeline)
        print("Pipeline:%r\n" % res)
        res.name = 'test pipeline 1'
        res = api_instance.pipelines_id_put(res.id, res)
        print("Pipeline updated: %r\n" % res)
    if False:
        res = api_instance.workflows_get()
        print("Workflows=%r\n" % res)
        sample_workflow = res[0]
        sample_workflow.name = 'test workflow'
        sample_workflow.is_profile = True
        res = api_instance.workflows_post(sample_workflow)
        print("Create sample workflow: %r\n" % res)
    if False:
        res = api_instance.profiles_ion_get()
        print("Profile ION=%r\n" % res)
        r2 = api_instance.profiles_ion_get(ion_profile_id=[res[0].id])
        print("Profile ION.%s=%r\n" % (res[0].id, r2))
        r2 = api_instance.profiles_ion_get(ion_profile_id=(res[0].id,
                                                           res[1].id))
        print("Profile ION.%s=%r\n" % ('two ids', r2))
        for p in res:
            if p.name in ['test ion profile', 'test ion profile 2']:
                r2 = api_instance.profiles_ion_delete([p.id])
                print("Deleted ION=%r\n" % r2)
        sample_ion_profile = res[0]
        res[0].name = 'test ion profile'
        r2 = api_instance.profiles_ion_post(sample_ion_profile)
        print("New ION=%r\n" % r2)
        r2.name = 'test ion profile 2'
        r2 = api_instance.profiles_ion_ion_profile_id_put(r2.id, r2)
        print("Updated ION=%r\n" % r2)
        sample_workflow = r2
        res = api_instance.workflow_instances_get()
        print("List Workflow Instances=%r\n" % res)
        res = api_instance.workflows_id_run_post(
            sample_workflow.id,
            dict(operationMode='NOT-PRODUCTION', serviceAccount='mcenter_user',
                 annotation='Here'))
        print("Run sample workflow: %r\n" % res)
    if False:
        res = api_instance.users_me_get()
        print("Users_Me=%r\n" % res)
        res = api_instance.users_get()
        print("Users=%r\n" % res)
    if False:
        res = api_instance.workflows_get()
        print("Workflows=%r\n" % res)
        ion_instance_id = res[0].workflow[0].pipeline_pattern_id
        res = api_instance.prepare_logs_post(ion_instance_id=ion_instance_id)
        print("PrepareLogs=%r\n" % res)
except ApiException as e:
    traceback.print_exc()

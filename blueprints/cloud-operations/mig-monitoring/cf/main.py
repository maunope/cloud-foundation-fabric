# Copyright 2021 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# https://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from googleapiclient import discovery
from googleapiclient.errors import HttpError
from oauth2client.client import GoogleCredentials


from google.api import label_pb2 as ga_label
from google.api import metric_pb2 as ga_metric
from google.cloud import monitoring_v3
from google.cloud import logging

import time
import json
import base64

from pprint import pprint

import argparse

class Error(Exception):
  pass

##constants
AUTOSCALER_MAX_INSTANCES_METRIC_TYPE = "custom.googleapis.com/autoscaling_policy_max_num_replicas"
AUTOSCALER_RECOMMENDEDSIZE_METRIC_TYPE = "custom.googleapis.com/autoscaling_policy_recommended_size"
IG_MANAGER_CURRENTACTIONS_METRIC_TYPE = "custom.googleapis.com/instance_group_manager_current_actions"
BE_INSTANCE_HEALTH_METRIC_TYPE = "custom.googleapis.com/backend_instance_health"
IG_INSTANCE_HEALTH_METRIC_TYPE = "custom.googleapis.com/autohealer_instance_status"

def extract_backend_services(backend_services_items,region):
    """
        given the items list from a backendServices().aggregatedList call and a selected region, outputs a flat list merging global and region items
    """
    try:
        merged_backendservices=[]
        if 'global' in backend_services_items and 'backendServices' in  backend_services_items['global']:
            merged_backendservices=merged_backendservices+backend_services_items['global']['backendServices']
        if 'backendServices' in backend_services_items['regions/'+region]:
            merged_backendservices=merged_backendservices+backend_services_items['regions/'+region]['backendServices']
        return merged_backendservices
    except KeyError as e:
        raise Error('Dictionary missing required key: %s' % e)
        
        
def extract_instancegroup_managers(instancesgroup_managers_items,region,zones):
    """
        given the items list from a instanceGroupManagers().aggregatedList call, a selected region and the list of its zones from zones().list, outputs a flat list merging zone and region items
    """
    try:
        merged_instancesgroup_managers=[]
        #merge regional and zonal instance group managers for the selected region   
        if 'instanceGroupManagers' in instancesgroup_managers_items['regions/'+region]:
            merged_instancesgroup_managers=merged_instancesgroup_managers+instancesgroup_managers_items['regions/'+region]['instanceGroupManagers']
        for zone in zones:
            if 'instanceGroupManagers' in instancesgroup_managers_items['zones/'+zone['name']]:
                merged_instancesgroup_managers=merged_instancesgroup_managers+instancesgroup_managers_items['zones/'+zone['name']]['instanceGroupManagers']
        return merged_instancesgroup_managers        
    except KeyError as e:
        raise Error('Dictionary missing required key: %s' % e)
        

def extract_autoscalers(autoscaler_items,region,zones):
    """
        given the items list from a autoscalers().aggregatedList call, a selected region and the list of its zones from zones().list, outputs a flat list merging zone and region items
    """
    try:
        merged_autoscalers=[]
        #merge global, regional and zonal autoscalers for the selected region  
        if 'autoscalers' in autoscaler_items['regions/'+region]:
            merged_autoscalers=merged_autoscalers+autoscaler_items['regions/'+region]['autoscalers']
        for zone in zones:
            if 'autoscalers' in autoscaler_items['zones/'+zone['name']]:
                merged_autoscalers=merged_autoscalers+autoscaler_items['zones/'+zone['name']]['autoscalers']
        return merged_autoscalers        
    except KeyError as e:
        raise Error('Dictionary missing required key: %s' % e)          

   
#todo this makes the CF easier to deploy, but introduces a side effect on the other hand, consider modifying to interrupt the CF execution gracefully if prereqs are not met
def create_mig_monitoring_metric_descriptors(monitoring_service, project,force_metric_descriptors_rebuild):
   """
       given a monitoring_v3 service Client  and a project id creates metric descriptors required for custom autoscaler metrics tracking if they don't exist
   """ 
  
   try:
       #do nothing if metric descriptor already exists                                                                                         
       if not force_metric_descriptors_rebuild:
           existing_metric_descriptors=monitoring_service.list_metric_descriptors(request={'filter': 'metric.type =one_of("'+AUTOSCALER_MAX_INSTANCES_METRIC_TYPE+'","'+AUTOSCALER_RECOMMENDEDSIZE_METRIC_TYPE+'","'+IG_MANAGER_CURRENTACTIONS_METRIC_TYPE+'","'+BE_INSTANCE_HEALTH_METRIC_TYPE+'","'+IG_INSTANCE_HEALTH_METRIC_TYPE+'")', 'name': 'projects/'+project })
           existing_metric_types=[]
           for descriptor in existing_metric_descriptors:
               existing_metric_types.append(descriptor.type)
                          
           if AUTOSCALER_MAX_INSTANCES_METRIC_TYPE in existing_metric_types and AUTOSCALER_RECOMMENDEDSIZE_METRIC_TYPE in existing_metric_types and IG_MANAGER_CURRENTACTIONS_METRIC_TYPE in existing_metric_types and BE_INSTANCE_HEALTH_METRIC_TYPE in existing_metric_types and IG_INSTANCE_HEALTH_METRIC_TYPE in existing_metric_types:
               #nothing to do, all descriptors exist already
               return  
   except Error as e:
     raise Error('Checking existence of custom metrics descriptors for project %s : %s' % (project,e))
   
   if force_metric_descriptors_rebuild:
       print("{\"severity\":\"WARNING\",\"message\":\"Forcing rebuild of managed instance groups custom metric desrcriptors for project "+project+", avoid setting force_metric_descriptors_rebuild flag to avoid performance impact\"}")
       
   #create common labels for autoscaler custom metric types
   descriptor_as = ga_metric.MetricDescriptor()
   descriptor_as.type = ""
   descriptor_as.metric_kind = ga_metric.MetricDescriptor.MetricKind.GAUGE
   descriptor_as.value_type = ga_metric.MetricDescriptor.ValueType.INT64
   descriptor_as.description = ""

   label = ga_label.LabelDescriptor()
   label.key = "location"
   label.value_type = ga_label.LabelDescriptor.ValueType.STRING
   label.description = "The zone or region for the autoscaler."
   descriptor_as.labels.append(label)

   label = ga_label.LabelDescriptor()
   label.key = "autoscaler_id"
   label.value_type = ga_label.LabelDescriptor.ValueType.STRING
   label.description = "The identifier for the autoscaler."
   descriptor_as.labels.append(label)

   label = ga_label.LabelDescriptor()
   label.key = "autoscaler_name"
   label.value_type = ga_label.LabelDescriptor.ValueType.STRING
   label.description = "The name of the autoscaler."
   descriptor_as.labels.append(label)  

   label = ga_label.LabelDescriptor()
   label.key = "instance_group_manager_id"
   label.value_type = ga_label.LabelDescriptor.ValueType.STRING
   label.description = "The identifier for the managed instance group scaled by the given autoscaler."
   descriptor_as.labels.append(label) 

   label = ga_label.LabelDescriptor()
   label.key = "instance_group_manager_name"
   label.value_type = ga_label.LabelDescriptor.ValueType.STRING
   label.description = "The name of the managed instance group scaled by the given autoscaler."
   descriptor_as.labels.append(label)    
   
   #create common labels for autoscaler custom metric types
   descriptor_igm = ga_metric.MetricDescriptor()
   descriptor_igm.type = ""
   descriptor_igm.metric_kind = ga_metric.MetricDescriptor.MetricKind.GAUGE
   descriptor_igm.value_type = ga_metric.MetricDescriptor.ValueType.INT64
   descriptor_igm.description = ""

   label = ga_label.LabelDescriptor()
   label.key = "location"
   label.value_type = ga_label.LabelDescriptor.ValueType.STRING
   label.description = "The zone or region for the instance group manager."
   descriptor_igm.labels.append(label)

   label = ga_label.LabelDescriptor()
   label.key = "instance_group_manager_id"
   label.value_type = ga_label.LabelDescriptor.ValueType.STRING
   label.description = "The identifier for the managed instance group manager."
   descriptor_igm.labels.append(label) 

   label = ga_label.LabelDescriptor()
   label.key = "instance_group_manager_name"
   label.value_type = ga_label.LabelDescriptor.ValueType.STRING
   label.description = "The name of the managed instance group manager"
   descriptor_igm.labels.append(label)
    
   label.key = "instance_group_manager_action"
   label.value_type = ga_label.LabelDescriptor.ValueType.STRING
   label.description = "Instance group manager action, e.g. creating,abbandoning,deleting"
   descriptor_igm.labels.append(label)
   
   #create common labels for instances health measured by backend healthcheck
   descriptor_beh = ga_metric.MetricDescriptor()
   descriptor_beh.type = ""
   descriptor_beh.metric_kind = ga_metric.MetricDescriptor.MetricKind.GAUGE
   descriptor_beh.value_type = ga_metric.MetricDescriptor.ValueType.INT64
   descriptor_beh.description = ""

   label = ga_label.LabelDescriptor()
   label.key = "location"
   label.value_type = ga_label.LabelDescriptor.ValueType.STRING
   label.description = "The zone or region for the monitored instances group manager."
   descriptor_beh.labels.append(label)

   label = ga_label.LabelDescriptor()
   label.key = "backend_service_id"
   label.value_type = ga_label.LabelDescriptor.ValueType.STRING
   label.description = "The identifier for the backend"
   descriptor_beh.labels.append(label)

   label = ga_label.LabelDescriptor()
   label.key = "backend_service_name"
   label.value_type = ga_label.LabelDescriptor.ValueType.STRING
   label.description = "The name of the backend."
   descriptor_beh.labels.append(label)  

   label = ga_label.LabelDescriptor()
   label.key = "instance_group_manager_id"
   label.value_type = ga_label.LabelDescriptor.ValueType.STRING
   label.description = "The identifier for the managed instance group ."
   descriptor_beh.labels.append(label) 

   label = ga_label.LabelDescriptor()
   label.key = "instance_group_manager_name"
   label.value_type = ga_label.LabelDescriptor.ValueType.STRING
   label.description = "The name of the managed instance group"
   descriptor_beh.labels.append(label)
   
   label = ga_label.LabelDescriptor()
   label.key = "backend_service_health_status"
   label.value_type = ga_label.LabelDescriptor.ValueType.STRING
   label.description = "Health status identifier"
   descriptor_beh.labels.append(label)
   
   
   #create common labels for instances health measured by autohealer health check
   descriptor_ahh = ga_metric.MetricDescriptor()
   descriptor_ahh.type = ""
   descriptor_ahh.metric_kind = ga_metric.MetricDescriptor.MetricKind.GAUGE
   descriptor_ahh.value_type = ga_metric.MetricDescriptor.ValueType.INT64
   descriptor_ahh.description = ""

   label = ga_label.LabelDescriptor()
   label.key = "location"
   label.value_type = ga_label.LabelDescriptor.ValueType.STRING
   label.description = "The zone or region for the monitored instances group manager."
   descriptor_ahh.labels.append(label)

   label = ga_label.LabelDescriptor()
   label.key = "instance_group_manager_id"
   label.value_type = ga_label.LabelDescriptor.ValueType.STRING
   label.description = "The identifier for the managed instance group ."
   descriptor_ahh.labels.append(label) 

   label = ga_label.LabelDescriptor()
   label.key = "instance_group_manager_name"
   label.value_type = ga_label.LabelDescriptor.ValueType.STRING
   label.description = "The name of the managed instance group"
   descriptor_ahh.labels.append(label)
   
   label = ga_label.LabelDescriptor()
   label.key = "autohealer_health_status"
   label.value_type = ga_label.LabelDescriptor.ValueType.STRING
   label.description = "Health status identifier"
   descriptor_ahh.labels.append(label)

   ##fill metric specific data and write
   try:
       descriptor_as.type = AUTOSCALER_MAX_INSTANCES_METRIC_TYPE
       descriptor_as.description = "Tracks the autoscaling policy max num replicas values for an autoscaler" 
       print("{\"severity\":\"NOTICE\",\"message\":\"Building "+AUTOSCALER_MAX_INSTANCES_METRIC_TYPE+" metric descriptor for project "+project+"\"}")
       descriptor_as = monitoring_service.create_metric_descriptor(name="projects/"+project, metric_descriptor=descriptor_as) 
       
       descriptor_as.type = AUTOSCALER_RECOMMENDEDSIZE_METRIC_TYPE  
       descriptor_as.description = "Tracks the recommended size of an autoscaler"
       print("{\"severity\":\"NOTICE\",\"message\":\"Building "+AUTOSCALER_RECOMMENDEDSIZE_METRIC_TYPE+" metric descriptor for project "+project+"\"}")
       descriptor_as = monitoring_service.create_metric_descriptor(name="projects/"+project, metric_descriptor=descriptor_as) 
       
       descriptor_igm.type = IG_MANAGER_CURRENTACTIONS_METRIC_TYPE  
       descriptor_igm.description = "Tracks the count of current actions for instance group managers VMs"
       print("{\"severity\":\"NOTICE\",\"message\":\"Building "+IG_MANAGER_CURRENTACTIONS_METRIC_TYPE+" metric descriptor for project "+project+"\"}")
       descriptor_igm = monitoring_service.create_metric_descriptor(name="projects/"+project, metric_descriptor=descriptor_igm)
       
       descriptor_beh.type = BE_INSTANCE_HEALTH_METRIC_TYPE  
       descriptor_beh.description = "Tracks the count of instance health statuses measured by a backend service health check"
       print("{\"severity\":\"NOTICE\",\"message\":\"Building "+BE_INSTANCE_HEALTH_METRIC_TYPE+" metric descriptor for project "+project+"\"}")
       descriptor_beh = monitoring_service.create_metric_descriptor(name="projects/"+project, metric_descriptor=descriptor_beh)
       
       descriptor_ahh.type = IG_INSTANCE_HEALTH_METRIC_TYPE  
       descriptor_ahh.description = "Tracks the count of instance health statuses measured by an autohealer health check"
       print("{\"severity\":\"NOTICE\",\"message\":\"Building "+IG_INSTANCE_HEALTH_METRIC_TYPE+" metric descriptor for project "+project+"\"}")
       descriptor_ahh = monitoring_service.create_metric_descriptor(name="projects/"+project, metric_descriptor=descriptor_ahh)
       
       
   except Error as e:
     raise Error('Checking creating custom metric descriptors for project %s : %s' % (project,e))


def record_custom_autoscaling_metrics(monitoring_service, project, enriched_instance_group_manager):
    '''
        given an enriched instance group manager object containing backend service, instances and autoscaler information, records
        custom metrics 
    '''
    try:            
        now = time.time()
        seconds = int(now)
        nanos = int((now - seconds) * 10 ** 9)
        interval = monitoring_v3.TimeInterval(
            {"end_time": {"seconds": seconds, "nanos": nanos}}
        )
        
        #create common structure and labels for autoscaler custom time series
        if "autoscaler" in enriched_instance_group_manager:
            series_as = monitoring_v3.TimeSeries()
            series_as.metric.type = ""    
            series_as.resource.labels["project_id"]=project        
            #todo should we abort if location is missing?
            if "zone" in enriched_instance_group_manager:
                 series_as.metric.labels["location"]=enriched_instance_group_manager["zone"]
            elif "region" in enriched_instance_group_manager:
                 series_as.metric.labels["location"]=enriched_instance_group_manager["region"]
            series_as.resource.type = "global"
            series_as.metric.labels["autoscaler_id"]=enriched_instance_group_manager["autoscaler"]["id"]
            series_as.metric.labels["autoscaler_name"]=enriched_instance_group_manager["autoscaler"]["name"]
            series_as.metric.labels["instance_group_manager_id"]=enriched_instance_group_manager["id"]
            series_as.metric.labels["instance_group_manager_name"]=enriched_instance_group_manager["name"]
        
        #create common structure and labels for instance group manager custom time series
        series_igm = monitoring_v3.TimeSeries()
        series_igm.metric.type = ""    
        series_igm.resource.labels["project_id"]=project        
        #todo should we abort if location is missing?
        if "zone" in enriched_instance_group_manager:
             series_igm.metric.labels["location"]=enriched_instance_group_manager["zone"]
        elif "region" in enriched_instance_group_manager:
             series_igm.metric.labels["location"]=enriched_instance_group_manager["region"]
        series_igm.resource.type = "global"
        series_igm.metric.labels["instance_group_manager_id"]=enriched_instance_group_manager["id"]
        series_igm.metric.labels["instance_group_manager_name"]=enriched_instance_group_manager["name"]
        series_igm.metric.labels["instance_group_manager_action"]=""
        
        
        #create labels for backend service health status metric
        series_beh = monitoring_v3.TimeSeries()
        series_beh.metric.type = BE_INSTANCE_HEALTH_METRIC_TYPE    
        series_beh.resource.labels["project_id"]=project        
        if "zone" in enriched_instance_group_manager:
             series_beh.metric.labels["location"]=enriched_instance_group_manager["zone"]
        elif "region" in enriched_instance_group_manager:
             series_beh.metric.labels["location"]=enriched_instance_group_manager["region"]
        series_beh.resource.type = "global"
        series_beh.metric.labels["instance_group_manager_id"]=enriched_instance_group_manager["id"]
        series_beh.metric.labels["instance_group_manager_name"]=enriched_instance_group_manager["name"]
        series_beh.metric.labels["backend_service_id"]=""
        series_beh.metric.labels["backend_service_name"]=""
        series_beh.metric.labels["backend_service_health_status"]=""
        
        
        #create labels for autohealer health status metric
        series_ahh = monitoring_v3.TimeSeries()
        series_ahh.metric.type = IG_INSTANCE_HEALTH_METRIC_TYPE    
        series_ahh.resource.labels["project_id"]=project        
        if "zone" in enriched_instance_group_manager:
             series_ahh.metric.labels["location"]=enriched_instance_group_manager["zone"]
        elif "region" in enriched_instance_group_manager:
             series_ahh.metric.labels["location"]=enriched_instance_group_manager["region"]
        series_ahh.resource.type = "global"
        series_ahh.metric.labels["instance_group_manager_id"]=enriched_instance_group_manager["id"]
        series_ahh.metric.labels["instance_group_manager_name"]=enriched_instance_group_manager["name"]
        series_ahh.metric.labels["autohealer_health_status"]=""
                
        
        
        if "autoscaler" in enriched_instance_group_manager:
            #complete with specific time series data and write,create_time_series does both creation and append
            if "autoscalingPolicy" in  enriched_instance_group_manager["autoscaler"] and "maxNumReplicas" in enriched_instance_group_manager["autoscaler"]["autoscalingPolicy"]:
                series_as.metric.type = AUTOSCALER_MAX_INSTANCES_METRIC_TYPE
                point = monitoring_v3.Point({"interval": interval, "value": {"int64_value": enriched_instance_group_manager["autoscaler"]["autoscalingPolicy"]["maxNumReplicas"]}})
                series_as.points = [point]
                monitoring_service.create_time_series(name="projects/"+project, time_series=[series_as])
        
            if "recommendedSize" in enriched_instance_group_manager["autoscaler"]:
                series_as.metric.type = AUTOSCALER_RECOMMENDEDSIZE_METRIC_TYPE
                point = monitoring_v3.Point({"interval": interval, "value": {"int64_value": enriched_instance_group_manager["autoscaler"]["recommendedSize"]}})
                series_as.points = [point]
                monitoring_service.create_time_series(name="projects/"+project, time_series=[series_as])
        
        
        #scroll through actions listed in the currentActions list
        for currentAction in enriched_instance_group_manager["currentActions"]:
           series_igm.metric.type = IG_MANAGER_CURRENTACTIONS_METRIC_TYPE      
           series_igm.metric.labels["instance_group_manager_action"]=currentAction
           point = monitoring_v3.Point({"interval": interval, "value": {"int64_value": enriched_instance_group_manager["currentActions"][currentAction]}})
           series_igm.points = [point]
           monitoring_service.create_time_series(name="projects/"+project, time_series=[series_igm])
        
        
        #scroll through the health states measured by health checks associated to backend services associated to the current instance group manager   
        #scroll through the health states measured by health checks associated to backend services associated to the current instance group manager   
        health_counters={} 
        if "backendServices" in enriched_instance_group_manager:
            for backendService in enriched_instance_group_manager["backendServices"]:
                if 'instances_health' in enriched_instance_group_manager["backendServices"][backendService]:
                    for instance in enriched_instance_group_manager["backendServices"][backendService]['instances_health']:
                        if instance['healthState'] in health_counters:
                            health_counters[instance['healthState']]= health_counters[instance['healthState']]+1
                        else:
                            health_counters[instance['healthState']]= 1        
            
                for health_counter in health_counters:
                    series_beh.metric.type = BE_INSTANCE_HEALTH_METRIC_TYPE      
                    series_beh.metric.labels["backend_service_health_status"]=health_counter
                    series_beh.metric.labels["backend_service_id"]=backendService
                    series_beh.metric.labels["backend_service_name"]=enriched_instance_group_manager["backendServices"][backendService]["name"]
                    point = monitoring_v3.Point({"interval": interval, "value": {"int64_value": health_counters[health_counter]}})
                    series_beh.points = [point]
                    monitoring_service.create_time_series(name="projects/"+project, time_series=[series_beh])
        
        #scroll through the health states measured by health checks associated to autohealers
        health_counters={} 
        if "instances" in enriched_instance_group_manager:
            for instance in enriched_instance_group_manager["instances"]:
                if 'instanceHealth' in instance:
                    for instanceHealth in instance['instanceHealth']:
                        detailedHealthState=instanceHealth['detailedHealthState']
                        if detailedHealthState in health_counters:
                            health_counters[detailedHealthState]= health_counters[detailedHealthState]+1
                        else:
                            health_counters[detailedHealthState]= 1      

            for health_counter in health_counters:
                series_ahh.metric.type = IG_INSTANCE_HEALTH_METRIC_TYPE      
                series_ahh.metric.labels["autohealer_health_status"]=health_counter
                point = monitoring_v3.Point({"interval": interval, "value": {"int64_value": health_counters[health_counter]}})
                series_ahh.points = [point]
                monitoring_service.create_time_series(name="projects/"+project, time_series=[series_ahh])
             
    except Error as e:
      raise Error('Error recording custom metrics in project %s : %s' % (project,e))
    

def http_check_migs(req):
  """ 
     HTTP capable method wrapping internal_check_migs
  Args:
      request (flask.Request): HTTP request object.
  Returns:
      enriched_instance_group_managers (dictionary): List of instances with status, current action, health for each Regional MIG in a region/project
  """
  enriched_instance_group_managers = {'instanceGroupManagers': {}, 'result': 'ok'}
  
 
  request_json = req.get_json()
  
  
  if req.args and 'region' in req.args and 'project' in req.args :
    project = req.args['project']
    region = req.args['region']
  elif request_json and 'region' in request_json and 'project' in request_json:
      project=request_json['project']
      region = request_json['region']
  else:
    enriched_instance_group_managers['result'] = 'ko, missing mandatory parameters, project and region'
    print("{\"severity\":\"ERROR\",\"message\":\"Cloud Function called without passing mandatory parameters project and region\"}")
    return enriched_instance_group_managers
  if not region or not project:
    print("{\"severity\":\"ERROR\",\"message\":\"Cloud Function called without passing mandatory parameters project and region\"}")
    enriched_instance_group_managers['result'] = 'ko, region and project parameters cannot be empty'
    return enriched_instance_group_managers
  
  if req.args and 'force_metric_descriptors_rebuild' in req.args:
    force_metric_descriptors_rebuild = req.args['force_metric_descriptors_rebuild']
  elif request_json and 'force_metric_descriptors_rebuild' in request_json:
    force_metric_descriptors_rebuild=request_json['force_metric_descriptors_rebuild']
  else:
    force_metric_descriptors_rebuild=0

  internal_check_migs(project, region, enriched_instance_group_managers, force_metric_descriptors_rebuild)
  return enriched_instance_group_managers
  

def pubsub_check_migs(event, context):
  """ 
     Pub/sub capable method wrapping internal_check_migs
  Args:
      event: oncoming pub/sub message, expected JSON formmat
      context
  Returns:
      enriched_instance_group_managers (dictionary): List of instances with status, current action, health for each Regional MIG in a region/project
  """
  enriched_instance_group_managers = {'instanceGroupManagers': {}, 'result': 'ok'}
  
  json_message = json.loads(base64.b64decode(event['data']).decode('utf-8'))
   
  if 'region' in json_message and 'project' in json_message:
    project = json_message['project']
    region = json_message['region']
  else:
    enriched_instance_group_managers['result'] = 'ko, missing mandatory parameters, project and region'
    print("{\"severity\":\"ERROR\",\"message\":\"Cloud Function called without passing mandatory parameters project and region\"}")
    return enriched_instance_group_managers
  if not region or not project:
    print("{\"severity\":\"ERROR\",\"message\":\"Cloud Function called without passing mandatory parameters project and region\"}")
    enriched_instance_group_managers['result'] = 'ko, region and project parameters cannot be empty'
    return enriched_instance_group_managers
  
  if 'force_metric_descriptors_rebuild' in json_message:
    force_metric_descriptors_rebuild = json_message['force_metric_descriptors_rebuild']
  else:
    force_metric_descriptors_rebuild=0

  internal_check_migs(project, region, enriched_instance_group_managers, force_metric_descriptors_rebuild)
  return enriched_instance_group_managers
  
  
  
  
def internal_check_migs(project, region, enriched_instance_group_managers,force_metric_descriptors_rebuild=0):   
  """ 
      given a project/region, records autoscaler metrics and instance group actions metrics not recorded by default in Cloud Monitoring:
      - Maximum and recommmended number on instances 
      - Number of instances for each current action (e.g. none, creating, deleting) 
      - Number of instances per health state as seen by associated backend services health checks
      - Number of isntances per health state as seen by associated autohealer health checks
      
      metrics descriptors are created if they aren't present already, setting force_metric_descriptors_rebuild to 1 forces their recreation
    
      returns the merged status of managed instance groups, backend services and  associated autoscalers and instances
      merged array contains a list of rinstancesgroup_managers, the first autoscaler associated to each, all backend services to which the instance group is associated, 
      and all information tracked with custom metrics 
      
      Limitations: only the first autoscaler for each instancesgroup_manager is considered
  Args:
      project: project id
      region: region name
      enriched_instance_group_managers: object to be populated
      force_metric_descriptors_rebuild: forces the rewriting of metrics descriptors when raised
  Returns:
      enriched_instance_group_managers (dictionary): List of instances with status, current action, health for each Regional MIG in a region/project
  """ 
  credentials = GoogleCredentials.get_application_default()
  service = discovery.build('compute', 'v1', credentials=credentials)
  #todo check if it's ok not to explicitly pass credentials
  monitoring_service = monitoring_v3.MetricServiceClient() 
  
  
  #retrieve backend services, instance group managers, autoscalers for the selected project
  #retrieve the zones of the selected region
  try:
    # get the list of backend services for a project, both global and regional  
    res_backend_services = service.backendServices().aggregatedList(project=project).execute()
    # get the list of regional instance group managers for a project/region
    res_instancesgroup_managers = service.instanceGroupManagers().aggregatedList(project=project).execute()
    #get the list of autoscalers for a project, both global and regional
    res_autoscalers = service.autoscalers().aggregatedList(project=project).execute()
    # get the list of zones in the requested region
    res_zones = service.zones().list(project=project,filter="region = \"https://www.googleapis.com/compute/v1/projects/"+project+"/regions/"+region+"\"").execute()
    zones=res_zones['items']
  except HttpError as e:
    raise Error('Error retrieving backend services for project %s : %s' % (project,e))
  
  

  #KO gracefully if this method is called on migs that aren't fully configured 
  #note that items list is never empty, it always contains one item for each region or zone, with or without subitems content
  if 'items' not in res_instancesgroup_managers:
     print("{\"severity\":\"WARNING\",\"message\":\"Unable to retrieve instance group managers, items list missing, make sure MIG configuration is complete before retrying\"}")
     enriched_instance_group_managers['result'] = 'ko, no instance group managers found in project '+project
     return enriched_instance_group_managers
  if 'items' not in res_backend_services:
      print("{\"severity\":\"WARNING\",\"message\":\"Unable to retrieve backend services list, items list missing\"}")
      enriched_instance_group_managers['result'] = 'ko, no backend services found in project '+project
      return enriched_instance_group_managers
  if 'items' not in res_autoscalers:
     print("{\"severity\":\"WARNING\",\"message\":\"Unable to retrieve autoscalers list, items list missing, make sure MIG configuration is complete before retrying\"}") 
     enriched_instance_group_managers['result'] = 'ko, no autoscalers found in project '+project
     return enriched_instance_group_managers
     
  #extract objects from regional and zonal levels, 
  backend_services=extract_backend_services(res_backend_services['items'],region)   
  instancesgroup_managers = extract_instancegroup_managers(res_instancesgroup_managers['items'],region,zones)
  autoscalers= extract_autoscalers(res_autoscalers['items'],region,zones)
  
  #quit if there's nothing to do
  if len(instancesgroup_managers) <= 0:
      print("{\"severity\":\"NOTICE\",\"message\":\"No instance group managers found in region "+region+", nothing to do\"}") 
      enriched_instance_group_managers['result'] = 'ko, no instance group managers found in project '+project+' in region '+region
      if force_metric_descriptors_rebuild:
          #if the rebuild flag is checked, build metric descriptors before quitting
          create_mig_monitoring_metric_descriptors(monitoring_service,project,force_metric_descriptors_rebuild)
      return enriched_instance_group_managers
  #not having autoscalers is a pefectly legit case, i.e. if all MIGs are stateful
  #not having backend services is a perfectly legit case, i.e. if the function is run before MIGs are associate to balancers
  
    
  #enrich instance group managers information with backends, autoscalers and managed instances
  for instancesgroup_manager in instancesgroup_managers:
    ##backends
    for backend_service in backend_services:
      for backend in backend_service['backends']:
          if ('group' in backend and backend['group'] == instancesgroup_manager['instanceGroup']):
              if not 'backendServices' in instancesgroup_manager:
                  instancesgroup_manager['backendServices']={}  
              instancesgroup_manager['backendServices'][backend_service['id']] = backend_service

    ##autoscalers
    for autoscaler in autoscalers:
        # only first autoscaler is considered
        if 'target' in autoscaler:
            try:
                if (instancesgroup_manager['selfLink'] == autoscaler['target']):
                    instancesgroup_manager['autoscaler'] = autoscaler
                    break
            except KeyError as e:
                raise Error('Dictionary missing required key: %s' % e)
            
    #current actions counts for instances in a MIG
    try:
      #instance group managers can be zonal or regional
      if "zone" in instancesgroup_manager:
          #turn "https://www.googleapis.com/compute/v1/projects/{project}/zones/europe-west1-b" in "europe-west1-b""
          zone=instancesgroup_manager['zone'].rsplit('/', 1)[-1]
          res_instances = service.instanceGroupManagers().listManagedInstances(project=project,instanceGroupManager=instancesgroup_manager['id'],zone=zone).execute()
      elif "region" in instancesgroup_manager:
          res_instances = service.regionInstanceGroupManagers().listManagedInstances(project=project,instanceGroupManager=instancesgroup_manager['id'],region=region).execute()
      else:
          raise Error('Error instance group manager  %s  in project %s has neither a region nor a zone defined'  %(instancesgroup_manager['id'],project))
    except HttpError as e:
      raise Error('Error retrieving managed instances for instances group manager %s  in project %s : %s' % (instancesgroup_manager['id'],project,e))
    try:
      instances_actions = res_instances['managedInstances']
    except KeyError as e:
      raise Error('Dictionary missing required key: %s' % e)
    
    #current health status for instances belonging to the backends associated to the instance group manager
    if ('backendServices' in instancesgroup_manager):     
        for backendService in instancesgroup_manager['backendServices']:
            try:
                res_instances_health = service.backendServices().getHealth(project=project,backendService=instancesgroup_manager['backendServices'][backendService]['id'],body={'group': instancesgroup_manager['instanceGroup']}).execute()
            except HttpError as e:
                raise Error('Error retrieving instances health for instances in backend   %s  in project %s : %s' % (instancesgroup_manager['backendService']['id'],project,e))
            try:
                instances_health = res_instances_health['healthStatus']
                instancesgroup_manager['backendServices'][backendService]['instances_health']=instances_health
            except KeyError as e:
                raise Error('Dictionary missing required key: %s' % e) 
            

    #build output object
    enriched_instance_group_managers['instanceGroupManagers'][instancesgroup_manager['selfLink']]=instancesgroup_manager
    enriched_instance_group_managers['instanceGroupManagers'][instancesgroup_manager['selfLink']]['instances']=instances_actions    
  
  if len(enriched_instance_group_managers['instanceGroupManagers']) > 0:
      #create custom metric descriptors if they don't exist 
      create_mig_monitoring_metric_descriptors(monitoring_service,project,force_metric_descriptors_rebuild)
  
  #record custom metrics
  for enriched_instance_group_manager in enriched_instance_group_managers['instanceGroupManagers']:
      record_custom_autoscaling_metrics(monitoring_service, project, enriched_instance_group_managers['instanceGroupManagers'][enriched_instance_group_manager])
    
  return(enriched_instance_group_managers)


"""
Copyright (c) 2017 5GTANGO
ALL RIGHTS RESERVED.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Neither the name of the 5GTANGO
nor the names of its contributors may be used to endorse or promote
products derived from this software without specific prior written
permission.

This work has been performed in the framework of the 5GTANGO project,
funded by the European Commission under Grant number 761493 through
the Horizon 2020 and 5G-PPP programmes. The authors would like to
acknowledge the contributions of their colleagues of the 5GTANGO
partner consortium (www.5gtango.eu).
"""
"""
This is SONATA's function lifecycle management plugin
"""

import os
from urllib.parse import urlparse

# List of topics that are used by the TAPI Wrapper for its rabbitMQ communication

VIM_LIST = 'infrastructure.wtapi.management.compute.list' #List Compute VIM

SVC_PREPARE = 'infrastructure.wtapi.service.prepare' #Prepare NFVI for service deployment
SVC_REMOVE = 'infrastructure.wtapi.service.remove' #Remove Service instance

WIM_LIST = 'infrastructure.wtapi.management.wan.list' #List WIMs
WIM_ATTACH = 'infrastructure.wtapi.management.wan.attach' #Link VIM to WIM
WIM_REMOVE = 'infrastructure.wtapi.management.wan.remove' #Remove WIM

WAN_CONFIGURE = 'infrastructure.wtapi.service.wan.configure' #Configure WAN for service instance
WAN_DECONFIGURE = 'infrastructure.wtapi.service.wan.deconfigure' #Deconfigure WAN for service instance

# Catalogue urls
cat_path = os.environ.get("cat_path").strip("/")
vnfd_ext = os.environ.get("vnfd_collection").strip("/")
nsd_ext = os.environ.get("nsd_collection").strip("/")

vnfd_path = cat_path + '/' + vnfd_ext
nsd_path = cat_path + '/' + nsd_ext


# Repository urls
repo_path = os.environ.get("repo_path").strip("/")
vnfr_ext = os.environ.get("vnfr_collection").strip("/")
nsr_ext = os.environ.get("nsr_collection").strip("/")


vnfr_path = repo_path + '/' + vnfr_ext
nsr_path = repo_path + '/' + nsr_ext

# Monitoring urls
monitoring_path = os.environ.get("monitoring_path").strip("/")

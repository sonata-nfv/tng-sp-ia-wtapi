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

import logging
import threading
import concurrent.futures as pool
import requests
import uuid
import time
import os
from os import path
import yaml, json
import uuid
from pprint import pprint

logging.basicConfig(level=logging.INFO)
logging.getLogger('pika').setLevel(logging.ERROR)
LOG = logging.getLogger("tapi-wrapper:tapi-wrapper")
LOG.setLevel(logging.DEBUG)
MAX_DEPLOYMENT_TIME = 5


class TapiWrapperEngine(object):

    def __init__(self, **kwargs):
        """
        Initialize TAPI WIM connection.
        :param app_id: string that identifies application

        """

        self.load_config()

        # Threading workers
        self.thrd_pool = pool.ThreadPoolExecutor(max_workers=100)
        # Track the workers
        self.tasks = []

    def load_config(self):
        """
        Read configuration file
        :return:
        """

        # connection_file = 'connections_commpilot.txt'
        # entities_file = 'entities.txt'
        #
        # # Get info from VIM and VNFD
        # with open(entities_file, 'r') as efp:
        #     self.entities = efp.readlines()
        #
        # # Get info from VLD -> this is done in main script
        # with open(connection_file, 'r') as cfp:
        #     self.virtual_links = cfp.readlines()

        self.wim_ip = os.getenv('WIM_IP', '10.1.1.54')
        self.wim_port = os.getenv('WIM_PORT', 9881)
        # self.wim_port = os.getenv('WIM_PORT', 8182)


    def get_topology(self):
        # nbi_topology_url = 'http://' + self.wim_ip + ':' + str(self.wim_port) + '/restconf/config/context/topology/0'
        tapi_topology_url = 'http://' + self.wim_ip + ':' + str(self.wim_port) + '/restconf/config/context/topology/0/'
        wim_topology = requests.get(tapi_topology_url)
        return wim_topology.json()

    def get_nodes(self):
        return

    def create_connectivity_service(self, uuid):
        """
        Call this function per virtual link
        :param uuid:
        :return:
        """
        LOG.info('Creating connectivity service')

        # nbi_base_call_url = 'http://{}:{}/restconf/config/calls/call/'.format(self.wim_ip, self.wim_port)
        tapi_cs_url = 'http://{}:{}/restconf/config/context/connectivity-service/'.format(
           self.wim_ip, self.wim_port)

        a_vnf = {
            'hw_addr': '00:1f:c6:9c:36:67'
        }
        z_vnf = {
            'hw_addr': 'fc:16:3e:f6:bb:c7'
        }

        a_cp = {
            'ref':'cp01',
            'hw_addr': '00:00:00:1b:21:7a:65:a8',
            'port': '3',
        }
        z_cp = {
            'ref':'cp21',
            'hw_addr': '00:00:00:1e:67:a1:8f:c1',
            'port': '6',
        }

        if a_cp['ref'] != z_cp['ref']:
            call_skeleton_l3 = {
                "callId": str(uuid),
                "contextId": "admin",
                "aEnd": {
                    "nodeId": a_cp['hw_addr'],
                    "edgeEndId": a_cp['port'],
                    "endpointId": a_cp['hw_addr'] + '_' + a_cp['port']
                },
                "zEnd": {
                    "nodeId": z_cp['hw_addr'],
                    "edgeEndId": z_cp['port'],
                    "endpointId": z_cp['hw_addr'] + '_' + z_cp['port']
                },
                "transportLayer": {
                    "layer": "ethernet",
                    "direction": "bidir"
                },
                "trafficParams": {
                    "reservedBandwidth": "50000"
                },
                "match": {
                    'ethSrc': a_vnf['hw_addr'] + '/ff:ff:ff:ff:ff:ff',
                    'ethDst': z_vnf['hw_addr'] + '/ff:ff:ff:ff:ff:ff'
                }
            }

            call_skeleton_l3_tapi = {
                    "end-point": [
                        {
                            "direction": "BIDIRECTIONAL",
                            "layer-protocol-name": "ETH",
                            "local-id": "csep-1",
                            "role": "SYMMETRIC",
                            "service-interface-point": "/restconf/config/context/service-interface-point/00:00:00:60:dd:45:c3:73_7"
                        },
                        {
                            "direction": "BIDIRECTIONAL",
                            "layer-protocol-name": "ETH",
                            "local-id": "csep-2",
                            "role": "SYMMETRIC",
                            "service-interface-point": "/restconf/config/context/service-interface-point/00:00:00:1e:67:a1:8d:45_7"
                        }
                    ],
                    "requested-capacity": {
                        "total-size": {
                            "unit": "GBPS",
                            "value": "1"
                        }
                    },
                    "service-type": "POINT_TO_POINT_CONNECTIVITY",
                    "uuid": uuid
                }

            # calls.append(str(call_skeleton_l3))
            LOG.debug('Q_net:{}'.format(call_skeleton_l3))
            headers = {
                'Content-type': 'application/json'
            }
            # response = requests.post(tapi_cs_url + call_skeleton_l3['callId'],
            #                          json=call_skeleton_l3_tapi,
            #                          headers=headers)
            response = requests.post(tapi_cs_url + uuid,
                                     json=call_skeleton_l3_tapi,
                                     headers=headers)
            LOG.debug('R_net:{}'.format(response.content))
            return response
        else:
            # TODO UPDATE REQUIRED, suggest PUT at an upper level?
            LOG.warning('Call is not required since connection point is the same')
            raise AttributeError

    def remove_connectivity_service(self, uuid):
        LOG.info('Removing connectivity service')
        # nbi_base_call_url = 'http://{}:{}/restconf/config/calls/call/'.format(self.wim_ip, self.wim_port)
        tapi_cs_url = 'http://{}:{}/restconf/config/context/connectivity-service/'.format(
            self.wim_ip, self.wim_port)
        headers = {
            'Accept': 'application/json'
        }
        response = requests.delete(tapi_cs_url + str(uuid), headers=headers)
        return response


test = TapiWrapperEngine()
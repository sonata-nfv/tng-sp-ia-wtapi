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

        wim_ip = os.environ.get('WIM_IP', '10.1.1.54')
        wim_port = os.environ.get('WIM_PORT', 9881)
        nbi_call_url = 'http://{}:{}/restconf/config/calls/call/'.format(wim_ip, wim_port)
        # tapi_cs_url = 'http://{}:8182/restconf/config/context/connectivity-service/'.format(wim_ip, wim_port)
        connection_file = 'connections_commpilot.txt'
        entities_file = 'entities.txt'
        call_index=50

        # Get info from VIM and VNFD
        with open(entities_file, 'r') as efp:
            self.entities = efp.readlines()

        # Get info from VLD -> this is done in main script
        with open(connection_file, 'r') as cfp:
            self.virtual_links = cfp.readlines()
        vnf_str = []
        cp_str = []

        for line in self.entities:
            line_split=line.split()
            if line_split[0] == 'vnf':
                vnf_str.append(
                    {
                        'name': line_split[1],
                        'net_addr': line_split[2],
                        'hw_addr': line_split[3],
                        'ref': line_split[4]
                    }
                )
            elif line.split()[0] == 'cp':
                cp_str.append(
                    {
                        'name': line_split[1],
                        'net_addr': line_split[2],
                        'hw_addr': '00:00:'+line_split[3],
                        'port': line_split[4],
                        'ref': line_split[5],
                        'extra': line_split[6]
                    }
                )
        calls = []
        for line in self.virtual_links:
            vl_pair=line.strip().split(',')
            # print(vl_pair)
            a_filter = list(filter(lambda vnf: vnf['name'] == vl_pair[0], vnf_str))
            if len(a_filter) > 0:
                a_vnf = a_filter[0]
                a_cp = list(filter(lambda cp: cp['name'] == a_vnf['ref'], cp_str))[0]
            else:
                a_cp = list(filter(lambda cp: cp['name'] == vl_pair[0], cp_str))[0]
                a_vnf = {'net_addr': a_cp['net_addr'],'hw_addr': a_cp['extra']}

            z_filter = list(filter(lambda vnf: vnf['name'] == vl_pair[1], vnf_str))
            if len(z_filter) > 0:
                z_vnf = z_filter[0]
                z_cp = list(filter(lambda cp: cp['name'] == z_vnf['ref'], cp_str))[0]
            else:
                z_cp = list(filter(lambda cp: cp['name'] == vl_pair[1], cp_str))[0]
                z_vnf = {'net_addr': z_cp['net_addr'],'hw_addr': z_cp['extra']}

            if a_cp['ref'] != z_cp['ref']:
                call_skeleton_l3 = {
                        "callId": str(call_index),
                        "contextId": "admin",
                        "aEnd": {
                            "nodeId": a_cp['hw_addr'],
                            "edgeEndId": a_cp['port'],
                            "endpointId": a_cp['hw_addr']+'_'+a_cp['port']
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

                call_skeleton_l2 = {
                    "callId": str(call_index + 100),
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
                        "reservedBandwidth": "1000"
                    },
                    "match": {
                        'ethType': 2054,
                        'arpSpa': a_vnf['net_addr'] + '/32',
                        'arpTpa': z_vnf['net_addr'] + '/32'
                    }
                }
                calls.append(str(call_skeleton_l3))
                calls.append(str(call_skeleton_l2))
                # print(call_skeleton_l3)
                LOG.debug('Q_net:{}'.format(call_skeleton_l3))
                headers = {
                    'Content-type': 'application/json'
                }
                response = requests.post(nbi_call_url + call_skeleton_l3['callId'],
                                         json=call_skeleton_l3,
                                         headers=headers)
                LOG.debug('R_net:{}'.format(response.content))

                LOG.debug('Q_link:{}'.format(call_skeleton_l2))
                response = requests.post(nbi_call_url + call_skeleton_l2['callId'],
                                         json=call_skeleton_l2,
                                         headers=headers)
                LOG.debug('R_link:{}'.format(response.content))
                call_index += 1
            else:
                print('Call is not required since connection point is the same')

            if 'a_vnf' in vars():
                del a_vnf
            if 'z_vnf' in vars():
                del z_vnf


    def get_topology(self):
        pass

    def get_nodes(self):
        pass

    def create_connectivity_service(self):
        LOG.info('Creating connectivity service')
        return

    def remove_connectivity_service(self):
        LOG.info('Removing connectivity service')
        return


test = TapiWrapperEngine()
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
        # self.thrd_pool.submit()
        # TODO: Create FSM-SSM static flow

    def submit(self, task, args):
        """
        Wrapper for submitting a task to the Executor (self.thrd_pool)
        :param task:
        :param args:
        :return:
        """
        self.tasks.append(task)
        if self.thrd_pool:
            self.thrd_pool.submit(task, args)

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

        # Populate entities with TapiWrapper.vim_info_get
        self.entities = [
            {
                'type':'vnf',
                'name':'telco-RP',
                'uuid':uuid.uuid4(),
                'datacenter':'core',
                'net_addr': '10.10.10.152',
                'hw_addr': 'fc:16:3e:bf:04:d3',
                'node': 'compute-1'
            },
            {
                'type': 'vnf',
                'name': 'telco-MS',
                'uuid': uuid.uuid4(),
                'datacenter': 'core',
                'net_addr': '10.10.10.151',
                'hw_addr': 'fc:16:3e:63:1b:04',
                'node': 'compute-1'
            },
            {
                'type': 'cp',
                'name': 'client-1',
                'uuid': uuid.uuid4(),
                'node_id': '00:00:00:1b:21:7a:65:a8',
                'edge_end_id': '3',
                'connection_point_id': 'cp01'
            },
            {
                'type': 'cp',
                'name': 'compute-1',
                'uuid': uuid.uuid4(),
                'node_id': '00:00:00:1e:67:a1:8f:c1',
                'edge_end_id': '6',
                'connection_point_id': 'cp21'
            }
        ]

    def get_topology(self):
        """
        Gets topology from WIM to match nodes
        :return:
        """
        # nbi_topology_url = 'http://' + self.wim_ip + ':' + str(self.wim_port) + '/restconf/config/context/topology/0'
        tapi_topology_url = 'http://' + self.wim_ip + ':' + str(self.wim_port) + '/restconf/config/context/topology/0/'
        wim_topology = requests.get(tapi_topology_url)
        return wim_topology.json()

    def generate_call_from_nap_pair(index, ingress_nap, egress_nap, ingress_ep, egress_ep,
                                    direction='unidir', layer='ethernet', reserved_BW=50000):
        """

        :param index:
        :param ingress_nap:
        :param egress_nap:
        :param ingress_ep:
        :param egress_ep:
        :param direction:
        :param layer:
        :param reserved_BW:
        :return call:
        """
        a_end = ingress_ep.split('_')
        z_end = egress_ep.split('_')
        if layer == 'ethernet' or (layer == 'mpls' and direction == 'unidir'):
            call = {
                "callId": str(index),
                "contextId": "admin",
                "aEnd": {
                    "nodeId": a_end[0],
                    "edgeEndId": a_end[1],
                    "endpointId": ingress_ep
                },
                "zEnd": {
                    "nodeId": z_end[0],
                    "edgeEndId": z_end[1],
                    "endpointId": egress_ep
                },
                "transportLayer": {
                    "layer": layer,
                    "direction": direction
                },
                "trafficParams": {
                    "reservedBandwidth": str(reserved_BW)
                },
                "match": {
                    'ipv4Src': ingress_nap,
                    'ipv4Dst': egress_nap
                }
            }
        elif layer == 'arp':
            call = {
                "callId": str(index),
                "contextId": "admin",
                "aEnd": {
                    "nodeId": a_end[0],
                    "edgeEndId": a_end[1],
                    "endpointId": ingress_ep
                },
                "zEnd": {
                    "nodeId": z_end[0],
                    "edgeEndId": z_end[1],
                    "endpointId": egress_ep
                },
                "transportLayer": {
                    "layer": 'ethernet',
                    "direction": direction
                },
                "trafficParams": {
                    "reservedBandwidth": str(reserved_BW)
                },
                "match": {
                    'ethType': 2054,
                    'arpSpa': ingress_nap,
                    'arpTpa': egress_nap
                }
            }
        elif layer == 'mpls_arp' and direction == 'unidir':
            call = {
                "callId": str(index),
                "contextId": "admin",
                "aEnd": {
                    "nodeId": a_end[0],
                    "edgeEndId": a_end[1],
                    "endpointId": ingress_ep
                },
                "zEnd": {
                    "nodeId": z_end[0],
                    "edgeEndId": z_end[1],
                    "endpointId": egress_ep
                },
                "transportLayer": {
                    "layer": 'mpls',
                    "direction": direction
                },
                "trafficParams": {
                    "reservedBandwidth": str(reserved_BW)
                },
                "match": {
                    'ethType': 2054,
                    'arpSpa': ingress_nap,
                    'arpTpa': egress_nap
                }
            }
        else:
            raise AttributeError
        return call

    def create_connectivity_service(self, uuid, a_cp, z_cp, a_vnf=None, z_vnf=None):
        """
        Call this function per virtual link
        :param uuid:
        :return:
        """
        LOG.info('Creating connectivity service')

        # nbi_base_call_url = 'http://{}:{}/restconf/config/calls/call/'.format(self.wim_ip, self.wim_port)
        tapi_cs_url = 'http://{}:{}/restconf/config/context/connectivity-service/'.format(
           self.wim_ip, self.wim_port)

        # a_vnf = {
        #     'hw_addr': '00:1f:c6:9c:36:67'
        # }
        # z_vnf = {
        #     'hw_addr': 'fc:16:3e:f6:bb:c7'
        # }

        # a_cp = {
        #     'ref':'cp01',
        #     'hw_addr': '00:00:00:1b:21:7a:65:a8',
        #     'port': '3',
        # }
        # z_cp = {
        #     'ref':'cp21',
        #     'hw_addr': '00:00:00:1e:67:a1:8f:c1',
        #     'port': '6',
        # }

        if a_cp['connection_point_id'] != z_cp['connection_point_id']:
            call_skeleton_l3 = {
                "callId": str(uuid),
                "contextId": "admin",
                "aEnd": {
                    "nodeId": a_cp['node_id'],
                    "edgeEndId": a_cp['edge_end_id'],
                    "endpointId": a_cp['node_id'] + '_' + a_cp['edge_end_id']
                },
                "zEnd": {
                    "nodeId": z_cp['node_id'],
                    "edgeEndId":z_cp['edge_end_id'],
                    "endpointId": z_cp['node_id'] + '_' + z_cp['edge_end_id']
                },
                "transportLayer": {
                    "layer": "ethernet",
                    "direction": "bidir"
                },
                "trafficParams": {
                    "reservedBandwidth": "50000"
                },
                "match": {
                    ('ethSrc' if a_vnf else None): (a_vnf['hw_addr'] + '/ff:ff:ff:ff:ff:ff' if a_vnf else None),
                    ('ethDst' if z_vnf else None): (z_vnf['hw_addr'] + '/ff:ff:ff:ff:ff:ff' if z_vnf else None),
                }
            }

            call_skeleton_l3_tapi = {
                    "end-point": [
                        {
                            "direction": "BIDIRECTIONAL",
                            "layer-protocol-name": "ETH",
                            "local-id": "csep-1",
                            "role": "SYMMETRIC",
                            "service-interface-point": "/restconf/config/context/service-interface-point/" +
                                                       a_cp['node_id'] + "_" + a_cp['edge_end_id']
                        },
                        {
                            "direction": "BIDIRECTIONAL",
                            "layer-protocol-name": "ETH",
                            "local-id": "csep-2",
                            "role": "SYMMETRIC",
                            "service-interface-point": "/restconf/config/context/service-interface-point/" +
                                                       z_cp['node_id'] + "_" + z_cp['edge_end_id']
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
            LOG.debug('Accesing {}'.format(tapi_cs_url + uuid + '/'))
            response = requests.post(tapi_cs_url + uuid + '/',
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
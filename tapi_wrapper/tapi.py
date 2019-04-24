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

import logging
import yaml
import time
import os
import requests
import copy
import uuid
import json
import threading
import sys
import concurrent.futures as pool
import psycopg2

from tapi_wrapper import messaging as messaging
from tapi_wrapper import tapi_helpers as tools
from tapi_wrapper import tapi_topics as topics
from tapi_wrapper import tapi_wrapper as engine
from tapi_wrapper.logger import TangoLogger

# logging.basicConfig(level=logging.DEBUG)
# LOG = logging.getLogger("tapi-wrapper:main")
# LOG.setLevel(logging.DEBUG)
LOG = TangoLogger.getLogger(__name__ + ':' + __file__, log_level=logging.DEBUG, log_json=True)


class TapiWrapper(object):
    """
    This class implements the WIM Tapi wrapper.
    """

    def __init__(self,
                 auto_register=True,
                 wait_for_registration=True,
                 start_running=True,
                 name="tapi",
                 version=None,
                 description=None):
        """
        Initialize class and son-mano-base.plugin.BasePlugin class.
        This will automatically connect to the broker, contact the
        plugin manager, and self-register this plugin to the plugin
        manager.

        :return:
        """

        # Create the ledger that saves state
        self.wtapi_ledger = {}

        self.thread_pool = pool.ThreadPoolExecutor(max_workers=10)

        base = 'amqp://' + 'guest' + ':' + 'guest'
        broker = os.environ.get("broker_host").split("@")[-1].split("/")[0]
        self.psql_user = os.environ.get('POSTGRES_USER')
        self.psql_pass = os.environ.get('POSTGRES_PASSWORD')
        self.url_base = base + '@' + broker + '/'

        self.name = "{}.{}".format(name, self.__class__.__name__)
        self.version = version
        self.description = description
        self.engine = engine.TapiWrapperEngine()
        #self.uuid = None  # uuid given by plugin manager on registration
        #self.state = None  # the state of this plugin READY/RUNNING/PAUSED/FAILED

        self.vim_map = [   # Get this mappings from vim registry
            # {'uuid': '9aea3a58-31e6-4c6a-a699-866399d651c0', 'location': 'core-datacenter'},
            {'uuid': '1111-22222222-33333333-4444', 'location': 'core-datacenter'},
            {'uuid': '94c261b4-357e-48ec-bb32-d1cb3da993b0', 'location': 'edge-datacenter'}
        ]

        LOG.info("Starting IA Wrapper: {} ...".format(self.name))
        # create and initialize broker connection
        while True:
            try:
                self.manoconn = messaging.ManoBrokerRequestResponseConnection(self.name)
                LOG.debug("Tapi plugin connected to broker")
                # NOTE: Is not yet connected since previous func is running in parallel, connection takes about 200 ms
                break
            except:
                time.sleep(0.1)
        # register subscriptions

        self.declare_subscriptions()

        if start_running:
            LOG.info("Tapi plugin running...")
            self.run()

    def run(self):
        """
        To be overwritten by subclass
        """
        # go into infinity loop (we could do anything here)
        # self.virtual_links_create(1234)
        while True:
            # test_engine = engine.TapiWrapperEngine()
            # engine.TapiWrapperEngine.create_connectivity_service(test_engine,'cs-plugin-1')
            # LOG.info('Conn service created, sleeping')
            # time.sleep(10)
            # engine.TapiWrapperEngine.remove_connectivity_service(test_engine,'cs-plugin-1')
            # LOG.info('Conn service removed, sleeping')
            time.sleep(0.01)
        # LOG.debug('Out of loop')

    def declare_subscriptions(self):
        """
        Declare topics that WTAPI subscribes on.
        """
        # # The topic on which wim domain capabilities are updated
        # self.manoconn.subscribe(self, topics.WAN_CONFIGURE)

        # The topic on which configure requests are posted.
        self.manoconn.subscribe(self.wan_network_configure, topics.WAN_CONFIGURE)
        LOG.info("Subscription to {} created".format(topics.WAN_CONFIGURE))

        # The topic on which release requests are posted.
        self.manoconn.subscribe(self.wan_network_deconfigure, topics.WAN_DECONFIGURE)
        LOG.info("Subscription to {} created".format(topics.WAN_DECONFIGURE))

##########################
# TAPI Threading management
##########################

    def get_connectivity_service(self, cs_id):
        return self.wtapi_ledger[cs_id]

    def get_services(self):
        return self.wtapi_ledger

    def get_capabilities(self, virtual_link_uuid):
        link_pairs = self.wtapi_ledger[virtual_link_uuid]['link_pairs']
        for link_pair in link_pairs:
            # Get NePs and PoPs from DB, correlate with WIM SIP DB and give capabilities per link
            # Insert capabilities to ledger
            pass
        return # {'result': True, 'message': f'wimregistry row created for {virtual_link_uuid}'}

    def start_next_task(self, virtual_link_uuid):
        """
        This method makes sure that the next task in the schedule is started
        when a task is finished, or when the first task should begin.
        :param func_id: the inst uuid of the function that is being handled.
        :param first: indicates whether this is the first task in a chain.
        """
        ns_uuid = self.wtapi_ledger[virtual_link_uuid]['service_uuid']
        try:
            # If the kill field is active, the chain is killed
            if self.wtapi_ledger[virtual_link_uuid]['kill_service']:
                # FIXME
                self.wtapi_ledger[virtual_link_uuid]['status'] = 'KILLING'

                LOG.info(f"Network Service #{ns_uuid}: Killing running workflow")
                # TODO: delete records, stop (destroy namespace)
                # TODO: Or, jump into the kill workflow.
                del self.wtapi_ledger[virtual_link_uuid]
                return

            # Select the next task, only if task list is not empty
            if len(self.wtapi_ledger[virtual_link_uuid]['schedule']) > 0:
                scheduled = self.wtapi_ledger[virtual_link_uuid]['schedule'].pop(0)
                LOG.debug('Network Service {}: Running {}'.format(virtual_link_uuid, scheduled))
                # share state with other WTAPI Wrappers (pop)
                next_task = getattr(self, scheduled)

                # Push the next task to the thread_pool
                # task = self.thread_pool.submit(next_task, (cs_id,))

                result = next_task(virtual_link_uuid)
                LOG.debug(f'Virtual link #{virtual_link_uuid} of Network Service #{ns_uuid}: '
                          f'Task finished, result: {result}')

                # Log if a task fails
                # if task.exception() is not None:
                #     LOG.debug(task.result())
                #
                # else:
                self.start_next_task(virtual_link_uuid)
            else:
                # del self.wtapi_ledger[cs_id]
                LOG.info(f"Virtual link #{virtual_link_uuid} of Network Service #{ns_uuid}: Schedule finished")
                return virtual_link_uuid
        except Exception as e:
            self.wtapi_ledger[virtual_link_uuid]['schedule'] = []
            self.tapi_error(virtual_link_uuid, e)


#############################
# TAPI Wrapper input - output
#############################

    def tapi_error(self, virtual_link_uuid, error=None):
        """
        This method is used to report back errors to the FLM
        """
        if error is None:
            ns_uuid = self.wtapi_ledger[virtual_link_uuid]["service_uuid"]
            error = self.wtapi_ledger[virtual_link_uuid]['error']
        LOG.error(f'Virtual link #{virtual_link_uuid} of Network Service #{ns_uuid}: error occured: {error}')

        message = {
            'request_status': 'FAILED',
            'error': error,
            'timestamp': time.time()
        }
        corr_id = self.wtapi_ledger[virtual_link_uuid]['orig_corr_id']
        topic = self.wtapi_ledger[virtual_link_uuid]['topic']

        self.manoconn.notify(topic,
                             yaml.dump(message),
                             correlation_id=corr_id)

    #############################
    # Callbacks
    #############################

    def insert_reference_database(self, virtual_link_uuid):
        # Insert created link into wimregistry
        connection = None
        cursor = None
        try:
            connection = psycopg2.connect(user=self.psql_user,
                                          password=self.psql_pass,
                                          host="son-postgres",
                                          port="5432",
                                          database="wimregistry")
            cursor = connection.cursor()
            wim_uuid = self.wtapi_ledger[virtual_link_uuid]['wim']['uuid']

            query = f"INSERT INTO service_instances (instance_uuid, wim_uuid) VALUES " \
                    f"('{virtual_link_uuid}', '{wim_uuid}');"
            LOG.debug(f'query: {query}')
            cursor.execute(query)
            connection.commit()
            return {'result': True, 'message': f'wimregistry row created for {virtual_link_uuid}'}
        except (Exception, psycopg2.Error) as error:
            LOG.error(error)
            return {'result': False, 'message': f'error inserting {virtual_link_uuid}', 'error': error}
        finally:
            # closing database connection.
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    def delete_reference_database(self, virtual_link_uuid):
        # Delete created link from wimregistry
        connection = None
        cursor = None
        virtual_link_ids = [virtual_link_uuid]
        virtual_link_ids.extend(self.wtapi_ledger[virtual_link_uuid]['related_vls'])

        try:
            connection = psycopg2.connect(user=self.psql_user,
                                          password=self.psql_pass,
                                          host="son-postgres",
                                          port="5432",
                                          database="wimregistry")
            cursor = connection.cursor()
            query = f"DELETE FROM service_instances WHERE instance_uuid IN '{tuple(virtual_link_ids)}';"
            LOG.debug(f'query: {query}')
            cursor.execute(query)
            connection.commit()
            return {'result': True, 'message': f'wimregistry deleted for {virtual_link_ids}'}
        except (Exception, psycopg2.Error) as error:
            LOG.error(error)
            return {'result': False, 'message': f'error deleting {virtual_link_ids}', 'error': error}
        finally:
            # closing database connection.
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    def clean_ledger(self, virtual_link_uuid):
        LOG.debug('Cleaning context of {}'.format(virtual_link_uuid))
        if self.wtapi_ledger[virtual_link_uuid]['schedule']:
            raise ValueError('Schedule not empty')
        elif self.wtapi_ledger[virtual_link_uuid]['active_connectivity_services']:
            raise ValueError('There are still active connectivity services')
        else:
            del self.wtapi_ledger[virtual_link_uuid]

    def get_wim_info(self, virtual_link_uuid):
        """
                This function retrieves endpoint and name for selected wim
                :param virtual_link_uuid:
                :return:
        """
        connection = None
        cursor = None
        try:
            connection = psycopg2.connect(user=self.psql_user,
                                          password=self.psql_pass,
                                          host="son-postgres",
                                          port="5432",
                                          database="wimregistry")
            cursor = connection.cursor()
            wim_uuid = self.wtapi_ledger[virtual_link_uuid]['wim']['uuid']
            query_wim = f"SELECT (name, endpoint) FROM wim WHERE uuid = '{wim_uuid}';"
            LOG.debug(f'query_wim: {query_wim}')
            cursor.execute(query_wim)
            resp = cursor.fetchall()
            LOG.debug(f"resp_wim: {resp}")
            # TODO Check resp len || multiple wims for a vim?
            clean_resp = resp[0][0][1:-1].split(',')
            wim_name = clean_resp[0]
            wim_endpoint = clean_resp[1]
            self.wtapi_ledger[virtual_link_uuid]['wim']['host'] = f'{wim_endpoint}:8182'
            self.wtapi_ledger[virtual_link_uuid]['wim']['name'] = wim_name
            return {'result': True, 'message': f'got wim {wim_name} for {virtual_link_uuid}'}
        except (Exception, psycopg2.Error) as error:
            LOG.error(error)
            return {'result': False, 'message': f'error getting {wim_uuid}', 'error': error}
        finally:
            # closing database connection.
            if cursor:
                cursor.close()
            if connection:
                connection.close()


    def get_endpoints_info(self, virtual_link_uuid):
        """
        This function retrieves info from endpoints (egress and ingress) attached in the MANO request
        :param virtual_link_uuid:
        :return:
        """
        LOG.debug(f'Network Service {virtual_link_uuid}: get_endpoints_info')
        ingress_endpoint_uuid = self.wtapi_ledger[virtual_link_uuid]['ingress']['location']
        egress_endpoint_uuid = self.wtapi_ledger[virtual_link_uuid]['egress']['location']
        connection = None
        cursor = None
        try:
            connection = psycopg2.connect(user=self.psql_user,
                                          password=self.psql_pass,
                                          host="son-postgres",
                                          port="5432",
                                          database="vimregistry")
            cursor = connection.cursor()
            query_ingress = f"SELECT (name, type) FROM vim WHERE uuid = '{ingress_endpoint_uuid}';"
            LOG.debug(f"query_ingress: {query_ingress}")
            cursor.execute(query_ingress)
            resp = cursor.fetchall()
            LOG.debug(f"response_ingress: {resp}")
            # TODO Check resp len || multiple wims for a vim?
            clean_resp = resp[0][0][1:-1].split(',')  # [('(NeP_1,endpoint)',)]
            ingress_name = clean_resp[0]  # Name is used to correlate with sips
            ingress_type = clean_resp[1]
            query_egress = f"SELECT (name, type) FROM vim WHERE uuid = '{egress_endpoint_uuid}';"
            LOG.debug(f"query_egress: {query_ingress}")
            cursor.execute(query_egress)
            resp = cursor.fetchall()
            LOG.debug(f"response_egress: {resp}")
            # TODO Check resp len || multiple wims for a vim?
            clean_resp = resp[0][0][1:-1].split(',')
            egress_name = clean_resp[0]
            egress_type = clean_resp[1]
            if not (ingress_name or egress_name):
                raise Exception('Both Ingress and Egress were not found in DB')
            elif not ingress_name:
                raise Exception('Ingress endpoint not found in DB')
            elif not egress_name:
                raise Exception('Egress endpoint not found in DB')
            self.wtapi_ledger[virtual_link_uuid]['ingress']['name'] = ingress_name
            self.wtapi_ledger[virtual_link_uuid]['ingress']['type'] = ingress_type
            self.wtapi_ledger[virtual_link_uuid]['egress']['name'] = egress_name
            self.wtapi_ledger[virtual_link_uuid]['egress']['type'] = egress_type
            return {
                'result': True,
                'message': f'got ingress {ingress_name} and egress {egress_name} for {virtual_link_uuid}',
                'error': None
            }
        except (Exception, psycopg2.Error) as error:
            LOG.error(error)
            return {
                'result': False,
                'message': f'error getting endpoints {ingress_endpoint_uuid} and {egress_endpoint_uuid}',
                'error': error
            }
        finally:
            # closing database connection.
            if cursor:
                cursor.close()
            if connection:
                connection.close()
        # FIXME raise error when no vims (result:False)
        # self.wtapi_ledger[virtual_link_uuid]['vim_name'] = list(
        #     filter(lambda x: x['uuid'] == vim_uuid, self.vim_map)
        # )[0]['location']
        #
        # return {'result':True, 'vim_name': self.wtapi_ledger[virtual_link_uuid]['vim_name']}

    def match_endpoints_with_sips(self, virtual_link_uuid):
        """
        Match each endpoint with it's corresponding sip
        :param virtual_link_uuid:
        :return:
        """
        wim_host = self.wtapi_ledger[virtual_link_uuid]['wim']['host']
        ingress_name = self.wtapi_ledger[virtual_link_uuid]['ingress']['name']
        egress_name = self.wtapi_ledger[virtual_link_uuid]['egress']['name']
        ingress_sip = self.engine.get_sip_by_name(wim_host, ingress_name)
        egress_sip = self.engine.get_sip_by_name(wim_host, egress_name)
        self.wtapi_ledger[virtual_link_uuid]['ingress']['sip'] = ingress_sip['name']  # value-name and value
        self.wtapi_ledger[virtual_link_uuid]['egress']['sip'] = egress_sip['name']
        return {
            'result': True,
            'message': f'got ingress {ingress_name} and egress {egress_name} sip match for {virtual_link_uuid}',
            'error': None
        }

    def virtual_links_create(self, virtual_link_uuid):
        """
        This function creates virtual links defined in nsd between each vnf and also between vnf and single endpoints
        :param virtual_link_uuid:
        :return:
        """
        ns_uuid = self.wtapi_ledger[virtual_link_uuid]["service_uuid"]
        LOG.debug(f"Creating virtual link {virtual_link_uuid} for network Service {ns_uuid}")
        # TODO now there is only one ingress and one egress
        # TODO: add latency param and qos in general if it's included in the request
        wim_host = self.wtapi_ledger[virtual_link_uuid]['wim']['host']
        ingress_nap = self.wtapi_ledger[virtual_link_uuid]['ingress']['nap']
        ingress_sip = self.wtapi_ledger[virtual_link_uuid]['ingress']['sip']
        egress_nap = self.wtapi_ledger[virtual_link_uuid]['egress']['nap']
        egress_sip = self.wtapi_ledger[virtual_link_uuid]['egress']['sip']
        if 'qos' in self.wtapi_ledger[virtual_link_uuid]:
            if 'bandwidth' in self.wtapi_ledger[virtual_link_uuid]['qos']:
                requested_capacity = float(self.wtapi_ledger[virtual_link_uuid]['qos']['bandwidth']) * 1e6
            else:
                requested_capacity = 1.5e9
            if 'latency' in self.wtapi_ledger[virtual_link_uuid]['qos']:
                requested_latency = self.wtapi_ledger[virtual_link_uuid]['qos']['latency']
            else:
                requested_latency = None
        else:
            # Best effort
            requested_capacity = 5e6
            requested_latency = None

        self.wtapi_ledger[virtual_link_uuid]['active_connectivity_services'] = []
        connectivity_services = [
            self.engine.generate_cs_from_nap_pair(
                ingress_nap, egress_nap,
                ingress_sip['value'], egress_sip['value'],
                layer='MPLS', direction='UNIDIRECTIONAL',
                requested_capacity=requested_capacity, latency=requested_latency),
            self.engine.generate_cs_from_nap_pair(
                ingress_nap, egress_nap,
                egress_sip['value'], ingress_sip['value'],
                layer='MPLS', direction='UNIDIRECTIONAL',
                requested_capacity=requested_capacity, latency=requested_latency),
            self.engine.generate_cs_from_nap_pair(
                ingress_nap, egress_nap,
                ingress_sip['value'], egress_sip['value'],
                layer='MPLS_ARP', direction='UNIDIRECTIONAL',
                requested_capacity=requested_capacity, latency=requested_latency),
            self.engine.generate_cs_from_nap_pair(
                ingress_nap, egress_nap,
                egress_sip['value'], ingress_sip['value'],
                layer='MPLS_ARP', direction='UNIDIRECTIONAL',
                requested_capacity=requested_capacity, latency=requested_latency),
        ]
        for connectivity_service in connectivity_services:
            try:
                self.engine.create_connectivity_service(wim_host, connectivity_service)
                self.wtapi_ledger[virtual_link_uuid]['active_connectivity_services'].append(
                    connectivity_service['uuid'])

            except Exception as exc:
                LOG.error(f'{connectivity_service["uuid"]} generated an exception: {exc}')
        # with pool.ThreadPoolExecutor(max_workers=100) as executor:
        #     futures_to_call = {
        #         executor.submit(self.engine.create_connectivity_service, (self.engine, call)): call['callId']
        #         for call in calls
        #     }
        #     for future in pool.as_completed(futures_to_call):
        #         call_id = futures_to_call[future]
        #         try:
        #             data = future.result()
        #         except Exception as exc:
        #             LOG.error('{} generated an exception: {}'.format(call_id, exc))
        return {'result': True, 'message': [cs['uuid'] for cs in connectivity_services]}

    def virtual_links_remove(self, virtual_link_uuid):
        """
        This function removes virtual links previously created
        :param virtual_link_uuid:
        :return:
        """
        LOG.debug('Network Service {}: Removing virtual links'.format(virtual_link_uuid))
        wim_host = self.wtapi_ledger[virtual_link_uuid]['wim']['host']

        # Gather all connectivity services
        if 'active_connectivity_services' in self.wtapi_ledger[virtual_link_uuid].keys():
            conn_services_to_remove = [
                {'cs_uuid': cs_uuid, 'vl_uuid': virtual_link_uuid}
                for cs_uuid in self.wtapi_ledger[virtual_link_uuid]['active_connectivity_services']
            ]
        else:
            conn_services_to_remove = []
            LOG.warning(f'Requested virtual_links_remove for {virtual_link_uuid} '
                        f'but there are no active connections')

        for rel_virtual_link_id in self.wtapi_ledger[virtual_link_uuid]['related_vls']:
            if 'active_connectivity_services' in self.wtapi_ledger[rel_virtual_link_id].keys():
                conn_services_to_remove.extend(
                    [
                        {'cs_uuid': cs_uuid, 'vl_uuid': rel_virtual_link_id}
                        for cs_uuid in self.wtapi_ledger[rel_virtual_link_id]['active_connectivity_services']
                    ]
                )
            else:
                LOG.warning(f'Requested virtual_links_remove for {rel_virtual_link_id} '
                            f'but there are no active connections')

        # Take all connectivity services down
        vl_removed = set()
        for cs in conn_services_to_remove:
            self.engine.remove_connectivity_service(wim_host, cs['cs_uuid'])
            vl_removed.update(cs['vl_uuid'])

        for virtual_link in vl_removed:
            self.wtapi_ledger[virtual_link]['active_connectivity_services'] = []

        return {'result': True, 'message': conn_services_to_remove, 'error': None}

    #############################
    # Subscription methods
    #############################

    def wan_network_configure(self, ch, method, properties, payload):
        """
        This function handles a received message on the *.service.wan.configure
        topic.
        payload:
            vl_id: '<uuid>' ;mandatory
            service_instance_id: '<uuid>' ;mandatory
            wim_uuid: '<uuid>' ;mandatory
            ingress:
              location: '<endpoint or vim identifier>' ;mandatory
              nap: '<ip segment>' ;mandatory
            egress:
              location: '<endpoint or vim identifier>' ;mandatory
              nap: '<ip segment>' ;mandatory
            qos:
              bandwidth: <integer>
              bandwidth_unit: '<unit, if absent default Mbps>'
              latency: <integer>
              latency_unit: '<unit, if absent default ms>'
            bidirectional: true
        """
        def send_error_response(error, virtual_link_uuid, scaling_type=None):

            response = {
                'error': error,
                'request_status': 'ERROR'
            }
            msg = ' Response on create request: ' + str(response)
            LOG.info('Virtual link ' + str(virtual_link_uuid) + msg)
            self.manoconn.notify(topics.WAN_CONFIGURE,
                                 yaml.dump(response),
                                 correlation_id=properties.correlation_id)

        # Don't trigger on self created messages
        if self.name == properties.app_id:
            return

        LOG.info("WAN configure request received")
        LOG.debug(f'Parameters:properties:{properties.__dict__},payload:{payload}')
        message = yaml.load(payload)
        LOG.debug(f'Serialized payload in python: {message}')

        # Check if payload and properties are ok
        if properties.correlation_id is None:
            error = 'No correlation id provided in header of request'
            send_error_response(error, None)
            return
        if not isinstance(message, dict):
            error = 'Payload is not a dictionary'
            send_error_response(error, None)
            return
        # if all({'service_instance_id', 'nap', 'vim_list', 'qos_parameters'}) not in message.keys():
        #     error = 'Payload should contain "service_instance_id", "wim_uuid", "nap", "vim_list", "qos_parameters"'
        #     send_error_response(error, None)
        #     return
        expected_keys = {'service_instance_id', 'vl_id', 'wim_uuid', 'ingress', 'egress', 'bidirectional'}
        # expected_keys = {'service_instance_id', 'nap', 'vim_list', 'qos_parameters'}
        if not all(key in message.keys() for key in expected_keys):
            error = f'Payload should contain at least {expected_keys}'
            LOG.error(message.keys())
            send_error_response(error, None)
            return

        virtual_link_id = message['vl_id']
        virtual_link_uuid = str(uuid.uuid4())
        service_instance_id = message['service_instance_id']

        # Schedule the tasks that the Wrapper should do for this request.
        schedule = [
            'get_wim_info',
            'get_endpoints_info',
            'match_endpoints_with_sips',
            'virtual_links_create',
            'insert_reference_database',
            'respond_to_request'
        ]

        LOG.debug(f"Services deployed {self.wtapi_ledger}")
        LOG.info(f"Enabling virtual link {virtual_link_id} for service {service_instance_id}")

        # We are suposing that only a unique WIM will serve one service_instance
        if virtual_link_uuid not in self.wtapi_ledger.keys():
            self.wtapi_ledger[virtual_link_uuid] = {
                'uuid': virtual_link_uuid,
                'vl_id': virtual_link_id,
                'service_uuid': service_instance_id,
                'wim': {'uuid': message['wim_uuid']},
                'egress': message['egress'],  # location=uuid, nap=ip
                'ingress': message['ingress'],
                'qos': message['qos'] if 'qos' in message.keys() else {},
                'bidirectional': message['bidirectional'],
                'status': 'INIT',
                'orig_corr_id': properties.correlation_id,
                'error': None,
                'message': None,
                'kill_service': False,
                'schedule': schedule,
                'topic': properties.reply_to,
            }

        msg = "New virtual link request received. Creating flows..."
        LOG.info(f"NS {service_instance_id}, VL:{virtual_link_id}: {msg}")
        # Start the chain of tasks
        self.start_next_task(virtual_link_uuid)

        return self.wtapi_ledger[virtual_link_uuid]['schedule']

    def wan_network_deconfigure(self, ch, method, properties, payload):
        """
        This function handles a received message on the *.service.wan.deconfigure
        topic.
        payload: { service_instance_id: :uuid:}
        """
        def send_error_response(error, virtual_link_uuid, scaling_type=None):

            response = {
                'error': error,
                'request_status': 'ERROR'
            }
            msg = ' Response on remove request: ' + str(response)
            LOG.info('Service ' + str(virtual_link_uuid) + msg)
            self.manoconn.notify(topics.WAN_DECONFIGURE,
                                 yaml.dump(response),
                                 correlation_id=properties.correlation_id)

        # Don't trigger on self created messages
        if self.name == properties.app_id:
            return
        LOG.info("WAN deconfigure request received")
        LOG.debug(f'Parameters:properties:{properties.__dict__},payload:{payload}')
        message = yaml.load(payload)
        LOG.debug(f'Serialized payload in python: {message}')

        # Check if payload and properties are ok.
        if not isinstance(message, dict):
            error = 'Payload is not a dictionary'
            send_error_response(error, None)
            return
        if 'service_instance_id' not in message.keys():
            error = 'Payload should contain "service_instance_id"'
            send_error_response(error, None)
            return
        if properties.correlation_id is None:
            error = 'No correlation id provided in header of request'
            send_error_response(error, None)
            return

        service_instance_id = message['service_instance_id']

        if 'vl_id' in message.keys():
            if isinstance(message['vl_id'], list):
                virtual_links = message['vl_id']
            else:
                virtual_links = [message['vl_id']]
        else:
            virtual_links = [virtual_link for virtual_link in self.wtapi_ledger if virtual_link['service_instance_id'] == service_instance_id]

        virtual_link_uuid = virtual_links.pop(0)

        self.wtapi_ledger[virtual_link_uuid]['related_vls'] = virtual_links  # Link other virtual_links related by service_instance_id




        # Schedule the tasks that the K8S Wrapper should do for this request.
        LOG.debug(f"Virtual links deployed {virtual_links} for service {service_instance_id}")
        add_schedule = [
            'virtual_links_remove',
            'delete_reference_database',
            'respond_to_request',
            'clean_ledger'
        ]

        self.wtapi_ledger[virtual_link_uuid]['schedule'].extend(add_schedule)
        self.wtapi_ledger[virtual_link_uuid]['topic'] = properties.reply_to
        self.wtapi_ledger[virtual_link_uuid]['orig_corr_id'] = properties.correlation_id

        msg = "Network service remove request received."
        LOG.info("Network Service {}: {}".format(service_instance_id, msg))
        # Start the chain of tasks
        self.start_next_task(virtual_link_uuid)

        return self.wtapi_ledger[virtual_link_uuid]['schedule']



    def no_resp_needed(self, ch, method, prop, payload):
        """
        Dummy response method when other component will send a response, but
        FLM does not need it
        """

        pass

    def ia_configure_response(self, ch, method, prop, payload):
        """

        :param ch:
        :param method:
        :param prop:
        :param payload:
        :return:
        """
        pass

    def ia_deconfigure_response(self, ch, method, prop, payload):
        pass

    def respond_to_request(self, virtual_link_uuid):
        """
        This method creates a response message for the sender of requests.
        """

        message = {
            'error': self.wtapi_ledger[virtual_link_uuid]['error'],
            'virtual_link_uuid': virtual_link_uuid,
        }

        if self.wtapi_ledger[virtual_link_uuid]['error'] is None:
            message["request_status"] = "COMPLETED"
        else:
            message["request_status"] = "FAILED"

        if self.wtapi_ledger[virtual_link_uuid]['message'] is not None:
            message["message"] = self.wtapi_ledger[virtual_link_uuid]['message']

        LOG.info("Generating response to the workflow request for {}".format(virtual_link_uuid))

        corr_id = self.wtapi_ledger[virtual_link_uuid]['orig_corr_id']
        topic = self.wtapi_ledger[virtual_link_uuid]['topic']
        message["timestamp"] = time.time()
        self.manoconn.notify(
            topic,
            yaml.dump(message),
            correlation_id=corr_id)


def main():
    """
    Entry point to start wrapper.
    :return:
    """
    # reduce messaging log level to have a nicer output for this wrapper
    logging.getLogger("tapi-wrapper:messaging").setLevel(logging.DEBUG)
    logging.getLogger("tapi-wrapper:plugin").setLevel(logging.DEBUG)
    # logging.getLogger("amqp-storm").setLevel(logging.DEBUG)
    # create our function lifecycle manager
    tapi = TapiWrapper()
    tapi.thread_pool.shutdown()
    sys.exit()


if __name__ == '__main__':
    main()

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
                LOG.debug("Wrapper is connected to broker.")
                # NOTE: Is not yet connected since previous func is running in parallel, connection takes about 200 ms
                break
            except:
                time.sleep(0.1)
        # register subscriptions

        self.declare_subscriptions()

        if start_running:
            LOG.info("Wrapper running...")
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

    def start_next_task(self, service_instance_id):
        """
        This method makes sure that the next task in the schedule is started
        when a task is finished, or when the first task should begin.
        :param func_id: the inst uuid of the function that is being handled.
        :param first: indicates whether this is the first task in a chain.
        """
        try:
            # If the kill field is active, the chain is killed
            if self.wtapi_ledger[service_instance_id]['kill_service']:
                self.wtapi_ledger[service_instance_id]['status'] = 'KILLING'
                LOG.info("Network Service " + service_instance_id + ": Killing running workflow")
                # TODO: delete records, stop (destroy namespace)
                # TODO: Or, jump into the kill workflow.
                del self.wtapi_ledger[service_instance_id]
                return

            # Select the next task, only if task list is not empty
            if len(self.wtapi_ledger[service_instance_id]['schedule']) > 0:
                scheduled = self.wtapi_ledger[service_instance_id]['schedule'].pop(0)
                LOG.debug('Network Service {}: Running {}'.format(service_instance_id, scheduled))
                # share state with other WTAPI Wrappers (pop)
                next_task = getattr(self, scheduled)

                # Push the next task to the thread_pool
                # task = self.thread_pool.submit(next_task, (cs_id,))

                result = next_task(service_instance_id)
                LOG.debug('Network Service {}: Task finished, result: {}'.format(service_instance_id, result))

                # Log if a task fails
                # if task.exception() is not None:
                #     LOG.debug(task.result())
                #
                # else:
                self.start_next_task(service_instance_id)
            else:
                # del self.wtapi_ledger[cs_id]
                LOG.info("Network Service {}: Schedule finished".format(service_instance_id))
                return service_instance_id
        except Exception as e:
            self.wtapi_ledger[service_instance_id]['schedule'] = []
            self.tapi_error(service_instance_id, e)


#############################
# TAPI Wrapper input - output
#############################

    def tapi_error(self, service_instance_id, error=None):
        """
        This method is used to report back errors to the FLM
        """
        if error is None:
            error = self.wtapi_ledger[service_instance_id]['error']
        LOG.error("Network Service " + service_instance_id + ": error occured: " + error)

        message = {
            'request_status': 'FAILED',
            'error': error,
            'timestamp': time.time()
        }
        corr_id = self.wtapi_ledger[service_instance_id]['orig_corr_id']
        topic = self.wtapi_ledger[service_instance_id]['topic']

        self.manoconn.notify(topic,
                             yaml.dump(message),
                             correlation_id=corr_id)

    #############################
    # Callbacks
    #############################

    def insert_reference_database(self, service_instance_id):
        try:
            connection = psycopg2.connect(user=self.psql_user,
                                          password=self.psql_pass,
                                          host="son-postgres",
                                          port="5432",
                                          database="wimregistry")
            cursor = connection.cursor()
            wim_uuid = self.wtapi_ledger[service_instance_id]['wim_uuid']
            query = f"INSERT INTO service_instances (instance_uuid, wim_uuid) VALUES " \
                    f"('{service_instance_id}', '{wim_uuid}');"
            cursor.execute(query)
            resp = cursor.fetchall()
            LOG.debug(resp)
            return {'result': True, 'message': f'wimregistry row created for {service_instance_id}'}
        except (Exception, psycopg2.Error) as error:
            exception_message = str(error)
            return {'result': False, 'message': f'error inserting {service_instance_id}', 'error': exception_message}
        finally:
            # closing database connection.
            if connection:
                cursor.close()
                connection.close()

    def delete_reference_database(self, service_instance_id):
        try:
            connection = psycopg2.connect(user=self.psql_user,
                                          password=self.psql_pass,
                                          host="son-postgres",
                                          port="5432",
                                          database="wimregistry")
            cursor = connection.cursor()
            query = f"DELETE FROM service_instances WHERE instance_uuid ='{service_instance_id}';"
            cursor.execute(query)
            resp = cursor.fetchall()
            LOG.debug(resp)
            return {'result': True, 'message': f'wimregistry deleted for {service_instance_id}'}
        except (Exception, psycopg2.Error) as error:
            exception_message = str(error)
            return {'result': False, 'message': f'error deleting {service_instance_id}', 'error': exception_message}
        finally:
            # closing database connection.
            if connection:
                cursor.close()
                connection.close()

    def clean_ledger(self, service_instance_id):
        LOG.debug('Cleaning context of {}'.format(service_instance_id))
        if self.wtapi_ledger[service_instance_id]['schedule']:
            raise ValueError('Schedule not empty')
        elif self.wtapi_ledger[service_instance_id]['active_connectivity_services']:
            raise ValueError('There are still active connectivity services')
        else:
            del self.wtapi_ledger[service_instance_id]

    def get_wim_info(self, service_instance_id):
        """
        This function retrieves info from deployed vnfs in the vim to map them into topology ports
        :param service_instance_id:
        :return:
        """
        try:
            connection = psycopg2.connect(user=self.psql_user,
                                          password=self.psql_pass,
                                          host="son-postgres",
                                          port="5432",
                                          database="wimregistry")
            cursor = connection.cursor()
            vim_uuid = self.wtapi_ledger[service_instance_id]['vim_list'][0]['uuid']
            query_wim = f"SELECT (wim_uuid) FROM attached_vim WHERE vim_uuid = '{vim_uuid}';"
            cursor.execute(query_wim)
            resp = cursor.fetchall()
            LOG.debug(f"query_wim: {resp}")
            # TODO Check resp len || multiple wims for a vim?
            wim_uuid = resp[0][0]
            query_endpoint = f"SELECT (endpoint) FROM wim WHERE uuid = '{wim_uuid}';"
            cursor.execute(query_endpoint)
            resp= cursor.fetchall()
            LOG.debug(f"query_endpoint: {resp}")
            wim_endpoint = resp[0][0]
            self.wtapi_ledger[service_instance_id]['wim'] = {
                'uuid': wim_uuid,
                'ip': wim_endpoint,
                'port': 8182
            }
            return {'result': True, 'message': f'got wim {wim_uuid} for {service_instance_id}'}
        except (Exception, psycopg2.Error) as error:
            LOG.error(error)
            exception_message = str(error)
            return {'result': False, 'message': f'error deleting {service_instance_id}', 'error': exception_message}
        finally:
            # closing database connection.
            if connection:
                cursor.close()
                connection.close()

    def get_vim_info(self, service_instance_id):
        """
        This function retrieves info from deployed vnfs in the vim to map them into topology ports
        :param service_instance_id:
        :return:
        """
        LOG.debug('Network Service {}: get_vim_info'.format(service_instance_id))
        vim_uuid = self.wtapi_ledger[service_instance_id]['vim_list'][0]['uuid']
        # FIXME raise error when no vims (result:False)
        self.wtapi_ledger[service_instance_id]['vim_name'] = list(
            filter(lambda x: x['uuid'] == vim_uuid, self.vim_map)
        )[0]['location']
        return {'result':True, 'vim_name': self.wtapi_ledger[service_instance_id]['vim_name']}

    def virtual_links_create(self, service_instance_id):
        """
        This function creates virtual links defined in nsd between each vnf and also between vnf and single endpoints
        :param service_instance_id:
        :return:
        """
        LOG.debug("Network Service {}: Creating virtual links".format(service_instance_id))
        # Per each VL, create a CS
        # Match each VNF with its CP

        ingress_list = self.wtapi_ledger[service_instance_id]['ingresses']
        egress_list = self.wtapi_ledger[service_instance_id]['egresses']
        vim_name = self.wtapi_ledger[service_instance_id]['vim_name']
        wim = self.wtapi_ledger[service_instance_id]['wim']
        egress_sip = self.engine.get_sip_by_name(wim, vim_name)
        self.wtapi_ledger[service_instance_id]['active_connectivity_services'] = []
        connectivity_services = []
        for ingress_point in ingress_list:
            ingress_sip = self.engine.get_sip_by_name(wim, ingress_point['location'])
            for egress_point in egress_list:
                # Creating unidirectional flows per each sip
                # TODO: add latency param
                connectivity_services.extend([
                    self.engine.generate_cs_from_nap_pair(
                        ingress_point['nap'], egress_point['nap'],
                        ingress_sip['name']['value'], egress_sip['name']['value'],
                        layer='MPLS',direction='UNIDIRECTIONAL', requested_capacity=1e6),
                    self.engine.generate_cs_from_nap_pair(
                        egress_point['nap'], ingress_point['nap'],
                        egress_sip['name']['value'], ingress_sip['name']['value'],
                        layer='MPLS', direction='UNIDIRECTIONAL', requested_capacity=1e6),
                    self.engine.generate_cs_from_nap_pair(
                        ingress_point['nap'], egress_point['nap'],
                        ingress_sip['name']['value'], egress_sip['name']['value'],
                        layer='mpls_arp', direction='UNIDIRECTIONAL', requested_capacity=1e3),
                    self.engine.generate_cs_from_nap_pair(
                        egress_point['nap'], ingress_point['nap'],
                        egress_sip['name']['value'], ingress_sip['name']['value'],
                        layer='mpls_arp', direction='UNIDIRECTIONAL', requested_capacity=1e3)
                ])
        # TODO: Flow to vrouter <- Provided by IA?
        for connectivity_service in connectivity_services:
            try:
                self.engine.create_connectivity_service(wim, connectivity_service)
                self.wtapi_ledger[service_instance_id]['active_connectivity_services'].append(
                    connectivity_service['uuid'])

            except Exception as exc:
                LOG.error('{} generated an exception: {}'.format(connectivity_service['uuid'], exc))
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
        return {'result': True, 'calls_created': [cs['uuid'] for cs in connectivity_services]}

    def virtual_links_remove(self, service_instance_id):
        """
        This function removes virtual links previously created
        :param service_instance_id:
        :return:
        """
        LOG.debug('Network Service {}: Removing virtual links'.format(service_instance_id))
        wim = self.wtapi_ledger[service_instance_id]['wim']
        if 'active_connectivity_services' in self.wtapi_ledger[service_instance_id].keys():
            for cs_uuid in self.wtapi_ledger[service_instance_id]['active_connectivity_services']:
                self.engine.remove_connectivity_service(wim, cs_uuid)
            self.wtapi_ledger[service_instance_id]['active_connectivity_services'] = []
        else:
            LOG.warning('Requested virtual_links_remove for {} but there are no active connections'.format(
                service_instance_id))

    def wan_network_configure(self, ch, method, properties, payload):
        """
        This function handles a received message on the *.service.wan.configure
        topic.
        payload:{
            service_instance_id: :uuid:,
            vim_list: [
                {uuid: :uuid:, order: :int:}
            ],
            nap: {
                ingresses: [
                    {location: :String:, nap: :l3addr:}
                ],
                egresses: [
                    {location: :String:, nap: :l3addr:}
                ],
        }
        """
        def send_error_response(error, service_instance_id, scaling_type=None):

            response = {
                'error': error,
                'request_status': 'ERROR'
            }
            msg = ' Response on create request: ' + str(response)
            LOG.info('Service ' + str(service_instance_id) + msg)
            self.manoconn.notify(topics.WAN_CONFIGURE,
                                 yaml.dump(response),
                                 correlation_id=properties.correlation_id)

        # Don't trigger on self created messages
        if self.name == properties.app_id:
            return

        LOG.info("WAN configure request received.")
        LOG.debug('Parameters:channel:{},method:{},properties:{},payload:{}'.format(ch, method, properties, payload))
        message = yaml.load(payload)

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
        expected_keys = {'service_instance_id', 'nap', 'vim_list',}
        # expected_keys = {'service_instance_id', 'nap', 'vim_list', 'qos_parameters'}
        if not all(key in message.keys() for key in expected_keys):
            error = f'Payload should contain {expected_keys}'
            LOG.error(message.keys())
            send_error_response(error, None)
            return

        service_instance_id = message['service_instance_id']

        # Schedule the tasks that the Wrapper should do for this request.
        add_schedule =[
            'get_wim_info',
            'get_vim_info',
            'virtual_links_create',
            'insert_reference_database',
            'respond_to_request'
        ]

        LOG.debug("Services deployed {}".format(self.wtapi_ledger))
        LOG.info("Enabling networking for service {}".format(service_instance_id))

        self.wtapi_ledger[service_instance_id]={
            'uuid': service_instance_id,
            'egresses': message['nap']['egresses'],
            'ingresses': message['nap']['ingresses'],
            'vim_list': message['vim_list'],
            'wim': {},
            'QoS':{'requested_banwidth': None, 'RTT': None},
            'status': 'INIT',
            'kill_service': False,
            'schedule': [],
            'orig_corr_id': properties.correlation_id,
            'topic': properties.reply_to,
            'error': None,
            'message': None

        }
        self.wtapi_ledger[service_instance_id]['schedule'].extend(add_schedule)

        msg = "New network service request received. Creating flows..."
        LOG.info("Network Service {}: {}".format(service_instance_id, msg))
        # Start the chain of tasks
        self.start_next_task(service_instance_id)

        return self.wtapi_ledger[service_instance_id]['schedule']

    def wan_network_deconfigure(self, ch, method, properties, payload):
        """
        This function handles a received message on the *.service.wan.deconfigure
        topic.
        payload: { service_instance_id: :uuid:}
        """
        def send_error_response(error, service_instance_id, scaling_type=None):

            response = {
                'error': error,
                'request_status': 'ERROR'
            }
            msg = ' Response on remove request: ' + str(response)
            LOG.info('Service ' + str(service_instance_id) + msg)
            self.manoconn.notify(topics.WAN_DECONFIGURE,
                                 yaml.dump(response),
                                 correlation_id=properties.correlation_id)

        # Don't trigger on self created messages
        if self.name == properties.app_id:
            return
        LOG.info("WAN deconfigure request received.")
        LOG.debug('Parameters:channel:{},method:{},properties:{},payload:{}'.format(ch, method, properties, payload))
        message = yaml.load(payload)

        if not isinstance(message, dict):
            error = 'Payload is not a dictionary'
            send_error_response(error, None)
            return
        if 'service_instance_id' not in message.keys():
            error = 'Payload should contain "service_instance_id"'
            send_error_response(error, None)
            return





        # Check if payload and properties are ok.
        if properties.correlation_id is None:
            error = 'No correlation id provided in header of request'
            send_error_response(error, None)
            return
        if not isinstance(message, dict):
            error = 'Payload is not a dictionary'
            send_error_response(error, None)
            return

        service_instance_id = message['service_instance_id']

        # Schedule the tasks that the K8S Wrapper should do for this request.
        LOG.debug("Services deployed {}".format(self.wtapi_ledger))
        add_schedule = [
            'virtual_links_remove',
            'respond_to_request',
            'delete_reference_database',
            'clean_ledger'
        ]

        self.wtapi_ledger[service_instance_id]['schedule'].extend(add_schedule)
        self.wtapi_ledger[service_instance_id]['topic'] = properties.reply_to
        self.wtapi_ledger[service_instance_id]['orig_corr_id'] = properties.correlation_id

        msg = "Network service remove request received."
        LOG.info("Network Service {}: {}".format(service_instance_id, msg))
        # Start the chain of tasks
        self.start_next_task(service_instance_id)

        return self.wtapi_ledger[service_instance_id]['schedule']

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

    def respond_to_request(self, service_instance_id):
        """
        This method creates a response message for the sender of requests.
        """

        message = {
            'error': self.wtapi_ledger[service_instance_id]['error'],
            'service_instance_id': service_instance_id,
        }

        if self.wtapi_ledger[service_instance_id]['error'] is None:
            message["request_status"] = "COMPLETED"
        else:
            message["request_status"] = "FAILED"

        if self.wtapi_ledger[service_instance_id]['message'] is not None:
            message["message"] = self.wtapi_ledger[service_instance_id]['message']

        LOG.info("Generating response to the workflow request for {}".format(service_instance_id))

        corr_id = self.wtapi_ledger[service_instance_id]['orig_corr_id']
        topic = self.wtapi_ledger[service_instance_id]['topic']
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

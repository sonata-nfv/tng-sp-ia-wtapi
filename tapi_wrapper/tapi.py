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

from tapi_wrapper import messaging as messaging
from tapi_wrapper import tapi_helpers as tools
from tapi_wrapper import tapi_topics as topics
from tapi_wrapper import tapi_wrapper as engine

logging.basicConfig(level=logging.DEBUG)
LOG = logging.getLogger("tapi-wrapper:main")
LOG.setLevel(logging.DEBUG)


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
            LOG.debug(result)

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


#############################
# TAPI Wrapper input - output
#############################

    def tapi_error(self, cs_id, error=None):
        """
        This method is used to report back errors to the FLM
        """
        if error is None:
            error = self.wtapi_ledger[cs_id]['error']
        LOG.error("Network Service " + cs_id + ": error occured: " + error)
        # LOG.info("Function " + func_id + ": informing FLM")

        message = {
            'status': 'failed',
            'error': error,
            'timestamp': time.time()
        }
        corr_id = self.wtapi_ledger[cs_id]['orig_corr_id']
        topic = self.wtapi_ledger[cs_id]['topic']

        self.manoconn.notify(topic,
                             yaml.dump(message),
                             correlation_id=corr_id)

    #############################
    # Callbacks
    #############################

    def vim_info_get(self, service_instance_id):
        """
        This function retrieves info from deployed vnfs in the vim to map them into topology ports
        :param service_instance_id:
        :return:
        """
        LOG.debug('Network Service {}: vim_info_get'.format(service_instance_id))
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
        egress_sip = self.engine.get_sip_by_name(vim_name)
        calls = []
        for ingress_point in ingress_list:
            ingress_sip = self.engine.get_sip_by_name(ingress_point['location'])
            for egress_point in egress_list:
                # Creating unidirectional flows per each sip
                calls.extend([
                    self.engine.generate_call_from_nap_pair(
                        ingress_point['nap'], egress_point['nap'],
                        ingress_sip['name']['value'], egress_sip['name']['value'],
                        layer='mpls',direction='unidir'),
                    self.engine.generate_call_from_nap_pair(
                        egress_point['nap'], ingress_point['nap'],
                        egress_sip['name']['value'], ingress_sip['name']['value'],
                        layer='mpls', direction='unidir'),
                    self.engine.generate_call_from_nap_pair(
                        ingress_point['nap'], egress_point['nap'],
                        ingress_sip['name']['value'], egress_sip['name']['value'],
                        layer='mpls_arp', direction='unidir', reserved_bw=1000),
                    self.engine.generate_call_from_nap_pair(
                        egress_point['nap'], ingress_point['nap'],
                        egress_sip['name']['value'], ingress_sip['name']['value'],
                        layer='mpls_arp', direction='unidir', reserved_bw=1000)
                ])
        # TODO: Flow to vrouter
        for call in calls:
            try:
                self.engine.create_connectivity_service(call)
            except Exception as exc:
                LOG.error('{} generated an exception: {}'.format(call['callId'], exc))
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
        return {'result': True, 'calls_created': str(len(calls))}

    def virtual_links_remove(self, service_instance_id):
        """
        This function removes virtual links previously created
        :param service_instance_id:
        :return:
        """
        LOG.debug('Network Service {}: Removing virtual links'.format(service_instance_id))
        # Remove target VLs

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
                'status': 'ERROR'
            }

            msg = ' Response on remove request: ' + str(response)
            LOG.info('Function ' + str(service_instance_id) + msg)
            self.manoconn.notify(topics.WAN_DECONFIGURE,
                                 yaml.dump(response),
                                 correlation_id=corr_id)

        # Don't trigger on self created messages
        if self.name == properties.app_id:
            return

        LOG.info("WAN configure request received.")
        LOG.debug('Parameters:channel:{},method:{},properties:{},payload:{}'.format(ch, method, properties, payload))
        message = yaml.load(payload)

        # Extract the correlation id
        corr_id = properties.correlation_id

        if corr_id is None:
            error = 'No correlation id provided in header of request'
            send_error_response(error, None)
            return

        if not isinstance(message, dict):
            error = 'Payload is not a dictionary'
            send_error_response(error, None)
            return

        service_instance_id = message['service_instance_id']


        # Schedule the tasks that the Wrapper should do for this request.
        add_schedule =[
            'vim_info_get',
            'virtual_links_create',
            'respond_to_request'
        ]

        LOG.info("Services deployed {}".format(self.wtapi_ledger))
        LOG.info("Enabling networking for service {}".format(service_instance_id))

        self.wtapi_ledger[service_instance_id]={
            'uuid': service_instance_id,
            'egresses': message['nap']['egresses'],
            'ingresses': message['nap']['ingresses'],
            'vim_list': message['vim_list'],
            'QoS':{'requested_banwidth': None, 'RTT': None},
            'status': 'INIT',
            'kill_service': False,
            'schedule': [],
            'orig_corr_id': corr_id,
            'topic': properties.reply_to,
            'error': None,
            'message': None

        }
        self.wtapi_ledger[service_instance_id]['schedule'].extend(add_schedule)

        msg = ": New network service request received. Creating flows..."
        LOG.info("Network Service {}: {}".format(service_instance_id,msg))
        # Start the chain of tasks
        self.start_next_task(service_instance_id)

        return self.wtapi_ledger[service_instance_id]['schedule']

    def wan_network_deconfigure(self, ch, method, properties, payload):
        """
        This function handles a received message on the *.service.wan.deconfigure
        topic.
        """
        def send_error_response(error, service_instance_id, scaling_type=None):

            response = {
                'error': error,
                'status': 'ERROR'
            }

            msg = ' Response on remove request: ' + str(response)
            LOG.info('Function ' + str(service_instance_id) + msg)
            self.manoconn.notify(topics.WAN_DECONFIGURE,
                                 yaml.dump(response),
                                 correlation_id=corr_id)

        # Don't trigger on self created messages
        if self.name == properties.app_id:
            return

        LOG.info("WAN deconfigure request received.")
        LOG.debug('Parameters:channel:{},method:{},properties:{},payload:{}'.format(ch, method, properties, payload))

        message = yaml.load(payload)

        # Check if payload and properties are ok.
        corr_id = properties.correlation_id

        if corr_id is None:
            error = 'No correlation id provided in header of request'
            send_error_response(error, None)
            return

        if not isinstance(message, dict):
            error = 'Payload is not a dictionary'
            send_error_response(error, None)
            return

        func_id = message['id']

        self.add_function_to_ledger(message, corr_id, func_id, topics.WAN_DECONFIGURE)

        # Schedule the tasks that the K8S Wrapper should do for this request.
        add_schedule = [
            'virtual_links_remove',
            'respond_to_request'
        ]

        self.functions[func_id]['schedule'].extend(add_schedule)

        msg = ": New kill request received."
        LOG.info("Function " + func_id + msg)
        # Start the chain of tasks
        # self.start_next_task(func_id)

        return self.functions[func_id]['schedule']

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
            message["status"] = "COMPLETED"
        else:
            message["status"] = "FAILED"

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

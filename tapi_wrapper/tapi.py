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
        self.functions = {}

        self.thread_pool = pool.ThreadPoolExecutor(max_workers=10)

        self.wtapi_ledger = {}

        self.quit_flag = False

        base = 'amqp://' + 'guest' + ':' + 'guest'
        broker = os.environ.get("broker_host").split("@")[-1].split("/")[0]
        self.url_base = base + '@' + broker + '/'

        self.name = "%s.%s" % (name, self.__class__.__name__)
        self.version = version
        self.description = description
        #self.uuid = None  # uuid given by plugin manager on registration
        #self.state = None  # the state of this plugin READY/RUNNING/PAUSED/FAILED

        LOG.info("Starting IA Wrapper: {} ...".format(self.name))
        # create and initialize broker connection
        while True:
            try:
                self.manoconn = messaging.ManoBrokerRequestResponseConnection(self.name)
                break
            except:
                time.sleep(1)
        # register subscriptions
        LOG.info("Wrapper is connected to broker.")

        self.declare_subscriptions()

        if start_running:
            LOG.info("Wrapper running...")
            try:
                self.run()
            except KeyboardInterrupt:
                self.quit_flag = True

    def run(self):
        """
        To be overwritten by subclass
        """
        # go into infinity loop (we could do anything here)
        while not self.quit_flag:
            test_engine = engine.TapiWrapperEngine()
            engine.TapiWrapperEngine.create_connectivity_service(test_engine,'cs-plugin-1')
            LOG.info('Conn service created, sleeping')
            time.sleep(30)
            engine.TapiWrapperEngine.remove_connectivity_service(test_engine,'cs-plugin-1')
            LOG.info('Conn service removed, sleeping')
            time.sleep(20)

        sys.exit()

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

    def get_ledger(self, func_id):
        return self.functions[func_id]

    def get_functions(self):
        return self.functions

    def set_functions(self, functions_dict):
        self.functions = functions_dict
        return

    def start_next_task(self, func_id):
        """
        This method makes sure that the next task in the schedule is started
        when a task is finished, or when the first task should begin.
        :param func_id: the inst uuid of the function that is being handled.
        :param first: indicates whether this is the first task in a chain.
        """

        # If the kill field is active, the chain is killed
        if self.functions[func_id]['kill_chain']:
            LOG.info("Function " + func_id + ": Killing running workflow")
            # TODO: delete records, stop (destroy namespace)
            # TODO: Or, jump into the kill workflow.
            del self.functions[func_id]
            return

        # Select the next task, only if task list is not empty
        if len(self.functions[func_id]['schedule']) > 0:

            # share state with other K8S Wrappers
            next_task = getattr(self,
                                self.functions[func_id]['schedule'].pop(0))

            # Push the next task to the threadingpool
            task = self.thread_pool.submit(next_task, func_id)

            # Log if a task fails
            if task.exception() is not None:
                print(task.result())

            # When the task is done, the next task should be started if no flag
            # is set to pause the chain.
            if self.functions[func_id]['pause_chain']:
                self.functions[func_id]['pause_chain'] = False
            else:
                self.start_next_task(func_id)
        else:
            del self.functions[func_id]

    def add_function_to_ledger(self, payload, corr_id, func_id, topic):
        """
        This method adds new functions with their specifics to the ledger,
        so other functions can use this information.

        :param payload: the payload of the received message
        :param corr_id: the correlation id of the received message
        :param func_id: the instance uuid of the function defined by SLM.
        """

        # Add the function to the ledger and add instance ids
        self.functions[func_id] = {}
        self.functions[func_id]['vnfd'] = payload['vnfd']
        self.functions[func_id]['id'] = func_id

        # Add the topic of the call
        self.functions[func_id]['topic'] = topic

        # Add to correlation id to the ledger
        self.functions[func_id]['orig_corr_id'] = corr_id

        # Add payload to the ledger
        self.functions[func_id]['payload'] = payload

        # Add the service uuid that this function belongs to
        self.functions[func_id]['service_instance_id'] = payload['service_instance_id']

        # Add the VIM uuid
        self.functions[func_id]['vim_uuid'] = payload['vim_uuid']

        # Create the function schedule
        self.functions[func_id]['schedule'] = []

        # Create the chain pause and kill flag

        self.functions[func_id]['pause_chain'] = False
        self.functions[func_id]['kill_chain'] = False

        self.functions[func_id]['act_corr_id'] = None
        self.functions[func_id]['message'] = None

        # Add error field
        self.functions[func_id]['error'] = None

        return func_id


#############################
# TAPI Wrapper input - output
#############################

    def tapi_error(self, func_id, error=None):
        """
        This method is used to report back errors to the FLM
        """
        if error is None:
            error = self.functions[func_id]['error']
        LOG.error("Function " + func_id + ": error occured: " + error)
        # LOG.info("Function " + func_id + ": informing FLM")

        message = {
            'status': 'failed',
            'error': error,
            'timestamp': time.time()
        }
        corr_id = self.functions[func_id]['orig_corr_id']
        topic = self.functions[func_id]['topic']

        self.manoconn.notify(topic,
                             yaml.dump(message),
                             correlation_id=corr_id)

    #############################
    # Callbacks
    #############################

    def vim_info_get(self, func_id):
        """
        This function retrieves info from deployed vnfs in the vim to map them into topology ports
        :param func_id:
        :return:
        """
        pass

    def virtual_links_create(self, func_id):
        """
        This function creates virtual links defined in nsd between each vnf and also between vnf and single endpoints
        :param func_id:
        :return:
        """
        pass

    def virtual_links_remove(self, func_id):
        """
        This function removes virtual links previously created
        :param func_id:
        :return:
        """
        pass

    def wan_network_configure(self, ch, method, properties, payload):
        """
        This function handles a received message on the *.service.wan.configure
        topic.
        """

        def send_error_response(error, func_id, scaling_type=None):

            response = {
                'error': error,
                'status': 'ERROR'
            }

            msg = ' Response on remove request: ' + str(response)
            LOG.info('Function ' + str(func_id) + msg)
            self.manoconn.notify(topics.WAN_DECONFIGURE,
                                 yaml.dump(response),
                                 correlation_id=corr_id)

        # Don't trigger on self created messages
        if self.name == properties.app_id:
            return

        LOG.info("WAN configure request received.")
        message = yaml.load(payload)

        # Extract the correlation id
        corr_id = properties.correlation_id

        func_id = message['id']

        # Add the function to the ledger
        self.add_function_to_ledger(message, corr_id, func_id, topics.WAN_CONFIGURE)

        # Schedule the tasks that the Wrapper should do for this request.
        add_schedule =[
            'vim_info_get',
            'virtual_links_create'
        ]

        LOG.info("Functions " + str(self.functions))
        LOG.info("Function " + func_id)
        self.functions[func_id]['schedule'].extend(add_schedule)

        msg = ": New instantiation request received. Instantiation started."
        LOG.info("Function " + func_id + msg)
        # Start the chain of tasks
        self.start_next_task(func_id)

        return self.functions[func_id]['schedule']

    def wan_network_deconfigure(self, ch, method, properties, payload):
        """
        This function handles a received message on the *.service.wan.deconfigure
        topic.
        """
        def send_error_response(error, func_id, scaling_type=None):

            response = {
                'error': error,
                'status': 'ERROR'
            }

            msg = ' Response on remove request: ' + str(response)
            LOG.info('Function ' + str(func_id) + msg)
            self.manoconn.notify(topics.WAN_DECONFIGURE,
                                 yaml.dump(response),
                                 correlation_id=corr_id)

        # Don't trigger on self created messages
        if self.name == properties.app_id:
            return

        LOG.info("WAN deconfigure request received.")
        message = yaml.load(payload)

        # Check if payload is ok.

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

        if 'cnf_id' not in message.keys():
            error = 'cnf_uuid key not provided'
            send_error_response(error, None)
            return

        func_id = message['cnf_id']

        if 'serv_id' not in message.keys():
            error = 'serv_id key not provided'
            send_error_response(error, func_id)

        if 'vim_id' not in message.keys():
            error = 'vim_id key not provided'
            send_error_response(error, func_id)

        cnf = self.functions[func_id]
        if cnf['error'] is not None:
            send_error_response(cnf['error'], func_id)

        if cnf['vnfr']['status'] == 'terminated':
            error = 'CNF is already terminated'
            send_error_response(error, func_id)

        # Schedule the tasks that the K8S Wrapper should do for this request.
        add_schedule = [
            'virtual_links_remove'
        ]
        # add_schedule.append('remove_cnf')
        # add_schedule.append('respond_to_request')

        self.functions[func_id]['schedule'].extend(add_schedule)

        msg = ": New kill request received."
        LOG.info("Function " + func_id + msg)
        # Start the chain of tasks
        self.start_next_task(func_id)

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

    def ia_deploy_response(self, ch, method, prop, payload):
        """
        This method handles the response from the IA on the
        vnf deploy request.
        """

        LOG.info("Payload of request: " + str(payload))

        inc_message = yaml.load(payload)

        func_id = tools.funcid_from_corrid(self.functions, prop.correlation_id)

        msg = "Response from IA on vnf deploy call received."
        LOG.info("Function " + func_id + msg)

        self.functions[func_id]['status'] = inc_message['request_status']

        if inc_message['request_status'] == "COMPLETED":
            LOG.info("Vnf deployed correctly")
            self.functions[func_id]["ia_vnfr"] = inc_message["vnfr"]
            self.functions[func_id]["error"] = None

            # TODO:Temporary fix for the HSP case, needs fixing in longterm
            if "ip_mapping" in inc_message.keys():
                mapping = inc_message["ip_mapping"]
                self.functions[func_id]["ip_mapping"] = mapping
            else:
                self.functions[func_id]["ip_mapping"] = []

        else:
            LOG.info("Deployment failed: " + inc_message["message"])
            self.functions[func_id]["error"] = inc_message["message"]
            topic = self.functions[func_id]['topic']
            self.tapi_error(func_id, topic)
            return

        self.start_next_task(func_id)

    def ia_remove_response(self, ch, method, prop, payload):
        """
        This method handles responses on IA VNF remove requests.
        """
        inc_message = yaml.load(payload)

        func_id = tools.funcid_from_corrid(self.functions, prop.correlation_id)

        msg = "Response from IA on vnf remove call received."
        LOG.info("Function " + func_id + msg)

        if inc_message['request_status'] == "COMPLETED":
            LOG.info("Vnf removal successful")
            self.functions[func_id]["vnfr"]["status"] = "terminated"

        else:
            msg = "Removal failed: " + inc_message["message"]
            LOG.info("Function " + func_id + msg)
            self.functions[func_id]["error"] = inc_message["message"]
            self.tapi_error(func_id, self.functions[func_id]['topic'])
            return

        self.start_next_task(func_id)

    def deploy_cnf(self, func_id):
        """
        #TODO not usable by WIM
        This methods requests the deployment of a cnf
        """
        function = self.functions[func_id]
        obj_deployment = engine.TapiWrapperEngine.deployment_object(self, func_id, function['vnfd'])

        LOG.info("Reply from Kubernetes" + str(obj_deployment))

        deployment_selector = obj_deployment.spec.template.metadata.labels.get("deployment")
        LOG.info("Deployment Selector: " + str(deployment_selector))

        LOG.info("function[vnfd]:" + str(function['vnfd']))
        obj_service=engine.TapiWrapperEngine.service_object(self, func_id, function['vnfd'], deployment_selector)
        LOG.info("Service Object:" + str(obj_service))
        LOG.info("Creating a Deployment")
        engine.TapiWrapperEngine.create_deployment(self, obj_deployment, "default")
        LOG.info("Creating a Service")
        engine.TapiWrapperEngine.create_service(self, obj_service, "default")

        outg_message = {}
        outg_message['vnfd'] = function['vnfd']
        outg_message['vnfd']['instance_uuid'] = function['id']
        outg_message['vim_uuid'] = function['vim_uuid']
        outg_message['service_instance_id'] = function['service_instance_id']

        payload = yaml.dump(outg_message)

        corr_id = str(uuid.uuid4())
        self.functions[func_id]['act_corr_id'] = corr_id

        msg = ": IA contacted for function deployment."
        LOG.info("Function " + func_id + msg)
        LOG.debug("Payload of request: " + payload)
        # Contact the IA
        self.manoconn.call_async(self.ia_deploy_response,
                                 topics.CNF_DEPLOY,
                                 payload,
                                 correlation_id=corr_id)
        LOG.info("CNF WAS DEPLOYED CORRECTLY")
        # Pause the chain of tasks to wait for response
        self.functions[func_id]['pause_chain'] = True

    def remove_vnf(self, func_id):
        """
        #TODO Not usable by wim
        This method request the removal of a vnf
        """

        function = self.functions[func_id]
        outg_message = {}
        outg_message["service_instance_id"] = function['serv_id']
        outg_message['vim_uuid'] = function['vim_uuid']
        outg_message['vnf_uuid'] = func_id

        payload = yaml.dump(outg_message)

        corr_id = str(uuid.uuid4())
        self.functions[func_id]['act_corr_id'] = corr_id

        msg = ": IA contacted for function removal."
        LOG.info("Function " + func_id + msg)
        LOG.debug("Payload of request: " + payload)
        # Contact the IA
        self.manoconn.call_async(
            self.ia_remove_response,
            topics.CNF_REMOVE,
            payload,
            correlation_id=corr_id)

        # Pause the chain of tasks to wait for response
        self.functions[func_id]['pause_chain'] = True

    def respond_to_request(self, func_id):
        """
        This method creates a response message for the sender of requests.
        """

        message = {}
        message["timestamp"] = time.time()
        message["error"] = self.functions[func_id]['error']
        message["vnf_id"] = func_id

        if self.functions[func_id]['error'] is None:
            message["status"] = "COMPLETED"
        else:
            message["status"] = "FAILED"

        if self.functions[func_id]['message'] is not None:
            message["message"] = self.functions[func_id]['message']

        LOG.info("Generating response to the workflow request")

        corr_id = self.functions[func_id]['orig_corr_id']
        topic = self.functions[func_id]['topic']
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


if __name__ == '__main__':
    main()

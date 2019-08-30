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
import ipaddress
import traceback

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
        self.aux_wtapi_ledger = {}

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
        self.init_setup()

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
        # The topic on which wim domain capabilities are updated
        self.manoconn.subscribe(self.wan_list_capabilites, topics.WAN_CAPABILITIES)
        LOG.info(f"Subscription to {topics.WAN_CAPABILITIES} created")

        # The topic on which configure requests are posted.
        self.manoconn.subscribe(self.wan_network_configure, topics.WAN_CONFIGURE)
        LOG.info(f"Subscription to {topics.WAN_CONFIGURE} created")

        # The topic on which release requests are posted.
        self.manoconn.subscribe(self.wan_network_deconfigure, topics.WAN_DECONFIGURE)
        LOG.info(f"Subscription to {topics.WAN_DECONFIGURE} created")

    #############################
    # TAPI Wrapper STARTUP
    #############################

    def init_setup(self):
        # TODO:
        # Retrieve tapi wims
        # Remove every vl associated to each tapi wim
        # Mirror tapi db to ia db
        LOG.debug('Tapi wrapper setup')
        wim_list = self.get_wims_setup()
        associated_endpoints = []
        new_endpoints = []
        self.aux_wtapi_ledger['router_cs_registry'] = []
        self.aux_wtapi_ledger['management_cs_registry'] = []
        for wim in wim_list:
            LOG.debug(f'Inserting {wim} sips into IADB')
            wim_host = ':'.join([wim[2], '8182'])
            sip_inv = self.engine.get_sip_inventory(wim_host)
            vim_inv = self.get_vims_setup()
            old_endpoints = self.clean_wim_old_attachments(wim[0])
            sip_name_list = [
                name['value'] for sip in sip_inv for name in sip['name']
                if name['value-name'] == 'public-name'
            ]
            self.clean_endpoints_from_vim_db([e[0] for e in old_endpoints], sip_name_list)
            for sip in sip_inv:
                LOG.debug(f'Processing sip {sip}')
                vim_match = self.check_sip_vim(sip, vim_inv)
                sip_name = [
                    name['value'] for name in sip['name']
                    if name['value-name'] == 'public-name'
                ].pop()
                if vim_match:
                    LOG.debug(f'Sip={sip_name} matching a vim')
                    associated_endpoints.append({
                        'vim_uuid': vim_match[0],
                        'vim_endpoint': vim_match[2],
                        'wim_uuid': wim[0]
                    })
                    management_registry = {
                        'wim': wim_host,
                        'vim': vim_match[0],
                        'subnets': []
                    }
                    # TODO: Check if management_flow_ip are present in configuration field
                    if 'management_flow_ip' in vim_match[3] and 'floating_ip_ranging' in vim_match[3]:
                        management_flow = {
                            'management_flow_ip': vim_match[3]['management_flow_ip'],
                            'floating_ip_ranging': vim_match[3]['floating_ip_ranging'],
                        }
                        platform_sip = [
                            sip for sip in sip_inv for name in sip['name']
                            if name['value-name'] == 'public-type' and name['value'] == 'SP-MANO-CSE'
                        ].pop()
                        # process floating_ip_ranging
                        floating_subnets = []
                        layer = self.get_connectivity_layer_by_sip_info(platform_sip, sip)
                        for ip_range in management_flow['floating_ip_ranging'].split(','):
                            ip_tokens = ip_range.split('-')
                            floating_subnets.extend(
                                self.tokenize_subnet_range(ip_tokens[0].strip(), ip_tokens[1].strip()))
                        for subnet in floating_subnets:
                            if layer == 'MPLS':
                                management_cs = [
                                    self.engine.generate_cs_from_nap_pair(
                                        '/'.join([vim_match[3]['management_flow_ip'], '32']),
                                        subnet,
                                        platform_sip['uuid'], sip['uuid'],
                                        layer=layer, direction='UNIDIRECTIONAL'
                                    ),
                                    self.engine.generate_cs_from_nap_pair(
                                        subnet,
                                        '/'.join([vim_match[3]['management_flow_ip'], '32']),
                                        sip['uuid'], platform_sip['uuid'],
                                        layer=layer, direction='UNIDIRECTIONAL'
                                    ),
                                    self.engine.generate_cs_from_nap_pair(
                                        '/'.join([vim_match[3]['management_flow_ip'], '32']),
                                        subnet,
                                        platform_sip['uuid'], sip['uuid'],
                                        layer='MPLS_ARP', direction='UNIDIRECTIONAL'
                                    ),
                                    self.engine.generate_cs_from_nap_pair(
                                        subnet,
                                        '/'.join([vim_match[3]['management_flow_ip'], '32']),
                                        sip['uuid'], platform_sip['uuid'],
                                        layer='MPLS_ARP', direction='UNIDIRECTIONAL'
                                    )
                                ]
                            elif layer == 'ETH':
                                management_cs = [
                                    self.engine.generate_cs_from_nap_pair(
                                        '/'.join([vim_match[3]['management_flow_ip'], '32']),
                                        subnet,
                                        platform_sip['uuid'], sip['uuid'],
                                        layer=layer, direction='UNIDIRECTIONAL'
                                    ),
                                    self.engine.generate_cs_from_nap_pair(
                                        subnet,
                                        '/'.join([vim_match[3]['management_flow_ip'], '32']),
                                        sip['uuid'], platform_sip['uuid'],
                                        layer=layer, direction='UNIDIRECTIONAL'
                                    ),
                                    self.engine.generate_cs_from_nap_pair(
                                        '/'.join([vim_match[3]['management_flow_ip'], '32']),
                                        subnet,
                                        platform_sip['uuid'], sip['uuid'],
                                        layer='ARP', direction='UNIDIRECTIONAL'
                                    ),
                                    self.engine.generate_cs_from_nap_pair(
                                        subnet,
                                        '/'.join([vim_match[3]['management_flow_ip'], '32']),
                                        sip['uuid'], platform_sip['uuid'],
                                        layer='ARP', direction='UNIDIRECTIONAL'
                                    )
                                ]
                            else:
                                LOG.warning('Layer not compatible')
                                break
                            self.engine.create_connectivity_service(wim_host, management_cs[0])
                            self.engine.create_connectivity_service(wim_host, management_cs[1])
                            self.engine.create_connectivity_service(wim_host, management_cs[2])
                            self.engine.create_connectivity_service(wim_host, management_cs[3])
                            subnet_registry = {
                                'subnet': subnet,
                                'cs_ref': [cs['uuid'] for cs in management_cs]
                            }
                            management_registry['subnets'].append(subnet_registry)
                        self.aux_wtapi_ledger['management_cs_registry'].append(management_registry)
                        # router_ext_ip: A.B.C.D
                        # management_flow_ip: A.B.C.D
                elif not vim_match:
                    LOG.debug(f'Inserting new sip={sip_name}')
                    new_uuid = uuid.uuid4()
                    sip_country = [
                        name['value'] for name in sip['name']
                        if name['value-name'] == 'public-country'
                    ]
                    LOG.debug(f'Inserting new sip={sip_name} country={sip_country}')
                    sip_city = [
                        name['value'] for name in sip['name']
                        if name['value-name'] == 'public-city'
                    ]
                    LOG.debug(f'Inserting new sip={sip_name} city={sip_city}')
                    new_endpoints.append({
                        'uuid': str(new_uuid),
                        'name': sip_name,
                        'city': sip_city.pop() if sip_city else '',
                        'country': sip_country.pop() if sip_country else ''
                    })
                    associated_endpoints.append({'vim_uuid': str(new_uuid), 'vim_endpoint': '', 'wim_uuid': wim[0]})
        LOG.debug(f'Populating vimregisry: {new_endpoints}')
        inserted_endpoints = self.populate_vim_database(new_endpoints)
        LOG.debug(f'Associating WIM with corresponding endpoints in wimregisry: {associated_endpoints}')
        attached_endpoints = self.attach_wim_to_endpoints(associated_endpoints, inserted_endpoints)
        LOG.debug(f'New vim endpoints: {inserted_endpoints}, attached_endpoints: {attached_endpoints}')

    def tokenize_subnet_range(self, start_ip, end_ip):
        """
        :param start_ip:
        :param end_ip:
        :return subnet matches:
        """
        ip_subnets_generator = ipaddress.summarize_address_range(
            ipaddress.ip_address(start_ip), ipaddress.ip_address(end_ip)
        )
        ip_subnets_list = [str(ipnet) for ipnet in ip_subnets_generator]
        # ip_num = int(ipaddress.ip_address(end_ip)) - int(ipaddress.ip_address(start_ip))
        # addressing_space = math.ceil(math.log(ip_num, 2))
        # subnet_address = ipaddress.ip_address(
        #     int(ipaddress.ip_address(start_ip)) >> addressing_space << addressing_space)
        # # TODO: be more restrictive on this net instead of taking the bigger one compatible
        # return ipaddress.ip_network(str(subnet_address) + '/' + str(32-addressing_space))
        return ip_subnets_list

    def check_sip_vim(self, sip, vim_inv):
        nfvi_pop_type = [
            name['value'] for name in sip['name']
            if name['value-name'] == 'public-type' and name['value'] == 'NFVI-PoP-CSE'
        ]
        if nfvi_pop_type:
            sip_name = [
                name['value'] for name in sip['name']
                if name['value-name'] == 'public-name'
            ].pop()
            for vim in vim_inv:
                if sip_name == vim[1]:
                    return vim
        return []

    def get_connectivity_layer_by_sip_info(self, source_sip, target_sip):
        """
        When the node is the same on both source and target, connectivity layer cannot be MPLS
        :param source_sip:
        :param target_sip:
        :return:
        """
        source_local_name = [name['value'] for name in source_sip['name'] if name['value-name'] == 'local-name'].pop()
        target_local_name = [name['value'] for name in target_sip['name'] if name['value-name'] == 'local-name'].pop()
        if source_local_name.split('_')[0] == target_local_name.split('_')[0]:
            return 'ETH'
        else:
            return 'MPLS'

    def get_wims_setup(self):
        connection = None
        cursor = None
        LOG.debug('Getting WIMs from DB')
        try:
            connection = psycopg2.connect(user=self.psql_user,
                                          password=self.psql_pass,
                                          host="son-postgres",
                                          port="5432",
                                          database="wimregistry")
            cursor = connection.cursor()
            query = "SELECT uuid, name, endpoint FROM wim WHERE vendor in ('Tapi', 'tapi');"
            LOG.debug(f'query: {query}')
            cursor.execute(query)
            wims = cursor.fetchall()
            LOG.debug(f'Found wims: {wims}')
            return wims
        except (Exception, psycopg2.Error) as error:
            LOG.error(error)
            return []
        finally:
            # closing database connection.
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    def get_vims_setup(self):
        connection = None
        cursor = None
        try:
            connection = psycopg2.connect(user=self.psql_user,
                                          password=self.psql_pass,
                                          host="son-postgres",
                                          port="5432",
                                          database="vimregistry")
            cursor = connection.cursor()
            query = "SELECT uuid, name, endpoint, configuration FROM vim WHERE vendor IN ('Heat', 'heat', 'Mock', 'mock');"
            LOG.debug(f'query: {query}')
            cursor.execute(query)
            vims = cursor.fetchall()
            LOG.debug(f'Found vims: {vims}')
            return vims
        except (Exception, psycopg2.Error) as error:
            LOG.error(error)
            return []
        finally:
            # closing database connection.
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    def clean_endpoints_from_vim_db(self, db_endpoints, sip_names=None):
        connection = None
        cursor = None
        try:
            connection = psycopg2.connect(user=self.psql_user,
                                          password=self.psql_pass,
                                          host="son-postgres",
                                          port="5432",
                                          database="vimregistry")
            cursor = connection.cursor()
            LOG.debug(f"Removing {db_endpoints} from vimregistry to avoid duplicates")
            if len(db_endpoints) == 1:
                query_delete = f"DELETE FROM vim WHERE vendor = 'endpoint' AND uuid in ({db_endpoints[0]})"
                LOG.debug(f'query_delete: {query_delete}')
                cursor.execute(query_delete)
                connection.commit()
            elif len(db_endpoints) > 1:
                query_delete = f"DELETE FROM vim WHERE vendor = 'endpoint' AND uuid in {tuple(db_endpoints)}"
                LOG.debug(f'query_delete: {query_delete}')
                cursor.execute(query_delete)
                connection.commit()
            # GET VIM endpoints
            if len(sip_names) == 1:
                query_names = f"SELECT uuid FROM vim WHERE vendor = 'endpoint' AND name in ({sip_names[0]})"
                LOG.debug(f'query_names: {query_names}')
                cursor.execute(query_names)
                db_endpoints_by_name = [e[0] for e in cursor.fetchall()]
                LOG.debug(f"Removing {db_endpoints_by_name} from vimregistry to avoid naming duplicates")
                if len(db_endpoints_by_name) == 1:
                    query_delete_by_name = f"DELETE FROM vim WHERE vendor = 'endpoint' AND uuid in ({db_endpoints_by_name[0]})"
                    LOG.debug(f'query_delete_by_name: {query_delete_by_name}')
                    cursor.execute(query_delete_by_name)
                    connection.commit()
                    db_endpoints.append(db_endpoints_by_name)
                elif len(db_endpoints_by_name) > 1:
                    query_delete_by_name = f"DELETE FROM vim WHERE vendor = 'endpoint' AND uuid in {tuple(db_endpoints_by_name)}"
                    LOG.debug(f'query_delete_by_name: {query_delete_by_name}')
                    cursor.execute(query_delete_by_name)
                    connection.commit()
                    db_endpoints.append(db_endpoints_by_name)
            elif len(sip_names) > 1:
                query_names = f"SELECT uuid FROM vim WHERE vendor = 'endpoint' AND name in {tuple(sip_names)}"
                LOG.debug(f'query_names: {query_names}')
                cursor.execute(query_names)
                db_endpoints_by_name = [e[0] for e in cursor.fetchall()]
                LOG.debug(f"Removing {db_endpoints_by_name} from vimregistry to avoid naming duplicates")
                if len(db_endpoints_by_name) == 1:
                    query_delete_by_name = f"DELETE FROM vim WHERE vendor = 'endpoint' AND uuid in ({db_endpoints_by_name[0]})"
                    LOG.debug(f'query_delete_by_name: {query_delete_by_name}')
                    cursor.execute(query_delete_by_name)
                    connection.commit()
                    db_endpoints.append(db_endpoints_by_name)
                elif len(db_endpoints_by_name) > 1:
                    query_delete_by_name = f"DELETE FROM vim WHERE vendor = 'endpoint' AND uuid in {tuple(db_endpoints_by_name)}"
                    LOG.debug(f'query_delete_by_name: {query_delete_by_name}')
                    cursor.execute(query_delete_by_name)
                    connection.commit()
                    db_endpoints.append(db_endpoints_by_name)
            return db_endpoints
        except (Exception, psycopg2.Error) as error:
            LOG.error(error)
            return []
        finally:
            # closing database connection.
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    def clean_wim_old_attachments(self, wim_uuid):
        connection = None
        cursor = None
        try:
            connection = psycopg2.connect(user=self.psql_user,
                                          password=self.psql_pass,
                                          host="son-postgres",
                                          port="5432",
                                          database="wimregistry")
            cursor = connection.cursor()
            query = f"SELECT vim_uuid FROM attached_vim WHERE wim_uuid = '{wim_uuid}'"
            LOG.debug(f'query: {query}')
            cursor.execute(query)
            db_endpoints = cursor.fetchall()
            LOG.debug(f"Remove {db_endpoints} from vimregistry to avoid duplicates")
            if wim_uuid:
                query_delete = f"DELETE FROM attached_vim WHERE wim_uuid = '{wim_uuid}'"
                LOG.debug(f'query_delete: {query_delete}')
                cursor.execute(query_delete)
                connection.commit()
            return db_endpoints
        except (Exception, psycopg2.Error) as error:
            LOG.error(error)
            return []
        finally:
            # closing database connection.
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    def get_connectivity_service(self, cs_id):
        # FIXME
        return self.wtapi_ledger[cs_id]

    def get_virtual_links(self):
        return self.wtapi_ledger

    def populate_vim_database(self, endpoint_list):
        connection = None
        cursor = None
        LOG.debug(f'Populating DB with new endpoints attached to wim, endpoints={endpoint_list}')
        try:
            connection = psycopg2.connect(user=self.psql_user,
                                          password=self.psql_pass,
                                          host="son-postgres",
                                          port="5432",
                                          database="vimregistry")
            cursor = connection.cursor()
            # vim_names = "','".join([endpoint['name'] for endpoint in endpoint_list])
            # safety_query = f"SELECT name FROM vim WHERE name IN ('{vim_names}')"
            # cursor.execute(safety_query)
            # db_endpoints = set([e[0] for e in cursor.fetchall()])
            # LOG.debug(f"Filtering {db_endpoints} to avoid duplicates")
            # filtered_endpoints = [endpoint for endpoint in endpoint_list if endpoint['name'] not in db_endpoints]
            if endpoint_list:
                query = f"INSERT INTO vim (uuid, type, vendor, city, country, name, endpoint, username, domain, " \
                        f"configuration, pass, authkey) VALUES "
                for endpoint in endpoint_list:
                    query += f"('{endpoint['uuid']}','endpoint','endpoint','{endpoint['city']}'," \
                             f"'{endpoint['country']}','{endpoint['name']}','','','','{{}}','',''),"
                query = query[:-1] + ';'
                LOG.debug(f'Populating DB query: {query}')
                cursor.execute(query)
                connection.commit()
            else:
                LOG.debug('No new endpoints')
            return [endpoint['uuid'] for endpoint in endpoint_list]
        except (Exception, psycopg2.Error) as error:
            LOG.error(error)
            return []
        finally:
            # closing database connection.
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    def attach_wim_to_endpoints(self, endpoint_list, new_endpoint_list):
        connection = None
        cursor = None
        LOG.debug(f'Attaching WIMs to each corresponding endpoint, endpoints={endpoint_list}')
        try:
            connection = psycopg2.connect(user=self.psql_user,
                                          password=self.psql_pass,
                                          host="son-postgres",
                                          port="5432",
                                          database="wimregistry")
            cursor = connection.cursor()
            # vim_uuids = "','".join([endpoint['vim_uuid'] for endpoint in endpoint_list])
            # safety_query = f"SELECT vim_uuid FROM attached_vim WHERE vim_uuid IN ('{vim_uuids}')"
            # cursor.execute(safety_query)
            # db_endpoints = set([e[0] for e in cursor.fetchall()])
            # LOG.debug(f"Filtering {db_endpoints} to avoid duplicates")
            # filtered_endpoints = [
            #     endpoint for endpoint in endpoint_list]
                # if endpoint['vim_uuid'] not in db_endpoints and endpoint['vim_uuid'] in new_endpoint_list]
            if endpoint_list:
                LOG.debug(f"Attaching {endpoint_list} after filter")
                query = f"INSERT INTO attached_vim (vim_uuid, vim_address, wim_uuid) VALUES "
                for endpoint in endpoint_list:
                    query += f"('{endpoint['vim_uuid']}', '{endpoint['vim_endpoint']}', '{endpoint['wim_uuid']}'),"
                query = query[:-1] + ';'
                LOG.debug(f'Attaching WIMs query: {query}')
                cursor.execute(query)
                connection.commit()
            else:
                LOG.debug('No new endpoints')
            return [endpoint['vim_uuid'] for endpoint in endpoint_list]
        except (Exception, psycopg2.Error) as error:
            LOG.error(error)
            return []
        finally:
            # closing database connection.
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    def get_capabilities(self, virtual_link_uuid):
        link_pairs = self.wtapi_ledger[virtual_link_uuid]['link_pairs']
        for link_pair in link_pairs:
            # Get NePs and PoPs from DB, correlate with WIM SIP DB and give capabilities per link
            # Insert capabilities to ledger
            pass
        return # {'result': True, 'message': f'wimregistry row created for {virtual_link_uuid}'}

    #############################
    # TAPI Wrapper input - output
    #############################

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
                LOG.debug(f'Virtual link {virtual_link_uuid}: Running {scheduled}')
                # share state with other WTAPI Wrappers (pop)
                next_task = getattr(self, scheduled)

                # Push the next task to the thread_pool
                # task = self.thread_pool.submit(next_task, (cs_id,))

                result = next_task(virtual_link_uuid)
                LOG.debug(f'Virtual link #{virtual_link_uuid} of Network Service #{ns_uuid}: '
                          f'Task {scheduled} finished, result: {result}')

                # Log if a task fails
                # if task.exception() is not None:
                #     LOG.debug(task.result())
                #
                # else:
                self.start_next_task(virtual_link_uuid)
            else:
                # del self.wtapi_ledger[cs_id]
                if self.wtapi_ledger[virtual_link_uuid]['status'] == 'INIT':
                    self.wtapi_ledger[virtual_link_uuid]['status'] = 'OPERATIONAL'
                elif self.wtapi_ledger[virtual_link_uuid]['status'] == 'TERMINATING':
                    for vl in self.wtapi_ledger[virtual_link_uuid]['related_cs_sets']:
                        self.clean_ledger(vl)
                        LOG.info(f"Connectivity service group {vl} terminated")
                LOG.info(f"Virtual link #{virtual_link_uuid} of Network Service #{ns_uuid}: Schedule finished")
                LOG.debug(f'FINAL_LEDGER_DEBUG: {self.wtapi_ledger}')
                LOG.debug(f'FINAL_AUX_LEDGER_DEBUG: {self.aux_wtapi_ledger}')
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
            if virtual_link_uuid in self.wtapi_ledger:
                if 'error' in self.wtapi_ledger[virtual_link_uuid]:
                    error = self.wtapi_ledger[virtual_link_uuid]['error']
                else:
                    error = f'UNKNOWN error in {virtual_link_uuid}'
        if virtual_link_uuid in self.wtapi_ledger:
            if 'service_uuid' in self.wtapi_ledger[virtual_link_uuid]:
                ns_uuid = self.wtapi_ledger[virtual_link_uuid]["service_uuid"]
            else:
                ns_uuid = 'UNKNOWN'

            if 'orig_corr_id' in self.wtapi_ledger[virtual_link_uuid]:
                corr_id = self.wtapi_ledger[virtual_link_uuid]['orig_corr_id']
            else:
                corr_id = str(uuid.uuid4())
            if 'topic' in self.wtapi_ledger[virtual_link_uuid]:
                topic = self.wtapi_ledger[virtual_link_uuid]['topic']
            else:
                topic = None

        else:
            ns_uuid = 'UNKNOWN'
            topic = None
            corr_id = str(uuid.uuid4())
            error = f'UNKNOWN error in {virtual_link_uuid}'

        LOG.error(f'Virtual link #{virtual_link_uuid} of Network Service #{ns_uuid}: error occured: {error}')

        message = {
            'request_status': 'ERROR',
            'message': format(error),
        }
        if topic:
            self.manoconn.notify(topic,
                                 yaml.dump(message),
                                 correlation_id=corr_id)
        else:
            LOG.error(f'Correct topic not found, {virtual_link_uuid}')

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
        # virtual_link_id = virtual_link_uuid
        virtual_link_ids = (self.wtapi_ledger[virtual_link_uuid]['related_cs_sets'])

        try:
            connection = psycopg2.connect(user=self.psql_user,
                                          password=self.psql_pass,
                                          host="son-postgres",
                                          port="5432",
                                          database="wimregistry")
            cursor = connection.cursor()
            if len(virtual_link_ids) > 1:
                query = f"DELETE FROM service_instances WHERE instance_uuid IN {tuple(virtual_link_ids)};"
            else:
                query = f"DELETE FROM service_instances WHERE instance_uuid = '{virtual_link_ids[0]}';"
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
        LOG.debug(f'Cleaning context of {virtual_link_uuid}')
        if self.wtapi_ledger[virtual_link_uuid]['schedule']:
            LOG.warning(f'VL {virtual_link_uuid} schedule not empty: '
                        f'{self.wtapi_ledger[virtual_link_uuid]["schedule"]}')
            raise ValueError('Schedule not empty')
        elif self.wtapi_ledger[virtual_link_uuid]['active_connectivity_services']:
            LOG.warning(f'VL {virtual_link_uuid} active_connectivity_services not empty: '
                      f'{self.wtapi_ledger[virtual_link_uuid]["active_connectivity_services"]}')
            raise ValueError('There are still active connectivity services')
        else:
            del self.wtapi_ledger[virtual_link_uuid]
            LOG.debug(f'VL {virtual_link_uuid} removed from LOCAL DB')

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
            query_wim = f"SELECT name, endpoint FROM wim WHERE uuid = '{wim_uuid}';"
            LOG.debug(f'query_wim: {query_wim}')
            cursor.execute(query_wim)
            resp = cursor.fetchall()
            LOG.debug(f"resp_wim: {resp}")
            # TODO Check resp len || multiple wims for a vim?
            wim_name = resp[0][0]
            wim_endpoint = resp[0][1]
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
        LOG.debug(f'Virtual link {virtual_link_uuid}: get_endpoints_info')
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
            query_ingress = f"SELECT name, type, configuration FROM vim WHERE uuid = '{ingress_endpoint_uuid}';"
            LOG.debug(f"query_ingress: {query_ingress}")
            cursor.execute(query_ingress)
            resp = cursor.fetchall()
            LOG.debug(f"response_ingress: {resp}")
            # TODO Check resp len || multiple wims for a vim?
            ingress_name = resp[0][0]  # Name is used to correlate with sips
            ingress_type = resp[0][1]
            ingress_conf = resp[0][2]
            query_egress = f"SELECT name, type, configuration FROM vim WHERE uuid = '{egress_endpoint_uuid}';"
            LOG.debug(f"query_egress: {query_egress}")
            cursor.execute(query_egress)
            resp = cursor.fetchall()
            LOG.debug(f"response_egress: {resp}")
            # TODO Check resp len || multiple wims for a vim?
            egress_name = resp[0][0]
            egress_type = resp[0][1]
            egress_conf = resp[0][2]
            if not (ingress_name or egress_name):
                raise Exception('Both Ingress and Egress were not found in DB')
            elif not ingress_name:
                raise Exception('Ingress endpoint not found in DB')
            elif not egress_name:
                raise Exception('Egress endpoint not found in DB')
            self.wtapi_ledger[virtual_link_uuid]['ingress']['name'] = ingress_name
            self.wtapi_ledger[virtual_link_uuid]['ingress']['type'] = ingress_type
            self.wtapi_ledger[virtual_link_uuid]['ingress']['conf'] = ingress_conf
            self.wtapi_ledger[virtual_link_uuid]['egress']['name'] = egress_name
            self.wtapi_ledger[virtual_link_uuid]['egress']['type'] = egress_type
            self.wtapi_ledger[virtual_link_uuid]['egress']['conf'] = egress_conf
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
        layer = self.get_connectivity_layer_by_sip_info(ingress_sip, egress_sip)
        self.wtapi_ledger[virtual_link_uuid]['ingress']['sip'] = ingress_sip['uuid']  # value-name and value
        self.wtapi_ledger[virtual_link_uuid]['egress']['sip'] = egress_sip['uuid']
        self.wtapi_ledger[virtual_link_uuid]['layer'] = layer
        return {
            'result': True,
            'message': f'got ingress {ingress_name} and egress {egress_name} sip match for {virtual_link_uuid}',
            'error': None
        }

    def check_router_connection(self, virtual_link_uuid):
        if self.wtapi_ledger[virtual_link_uuid]['ingress']['type'] == 'endpoint' \
                and self.wtapi_ledger[virtual_link_uuid]['egress']['conf'].get('router_ext_ip'):
            client_endpoint_uuid = self.wtapi_ledger[virtual_link_uuid]['ingress']['location']
            pop_uuid = self.wtapi_ledger[virtual_link_uuid]['egress']['location']
        elif self.wtapi_ledger[virtual_link_uuid]['egress']['type'] == 'endpoint' \
                and self.wtapi_ledger[virtual_link_uuid]['ingress']['conf'].get('router_ext_ip'):
            client_endpoint_uuid = self.wtapi_ledger[virtual_link_uuid]['egress']['location']
            pop_uuid = self.wtapi_ledger[virtual_link_uuid]['ingress']['location']
        elif self.wtapi_ledger[virtual_link_uuid]['ingress']['conf'].get('router_ext_ip') \
                and self.wtapi_ledger[virtual_link_uuid]['egress']['conf'].get('router_ext_ip'):
            # TODO: implement multi-NFVI-PoP router flow
            LOG.warning('No client endpoint found, both endpoints are NFVI-PoPs, '
                            'multi-vim wan provisioning not implemented yet')
            return
        else:
            LOG.info('No virtual router in NFVI-PoP')
            return
        for entry in self.aux_wtapi_ledger['router_cs_registry']:
            if client_endpoint_uuid == entry['client_endpoint_uuid'] and pop_uuid == entry['pop_uuid']:
                LOG.debug(f"Virtual router connectivity already provisioned for "
                          f"{self.wtapi_ledger[virtual_link_uuid]['ingress']['location']} "
                          f"and {self.wtapi_ledger[virtual_link_uuid]['egress']['location']}")
                self.wtapi_ledger[virtual_link_uuid]['router_flow_operational'] = True
                return

        # If code reaches this point, there's no client-pop routing cs - creation is needed
        self.wtapi_ledger[virtual_link_uuid]['router_flow_creation'] = True
        self.aux_wtapi_ledger['router_cs_registry'].append({
            'client_endpoint_uuid': client_endpoint_uuid,
            'pop_uuid': pop_uuid,
            'associated_cs': [],
            'routing_cs': [],
        })

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

        if len(self.wtapi_ledger[virtual_link_uuid]['ingress']['nap'].split('/')) == 2:
            ingress_nap = self.wtapi_ledger[virtual_link_uuid]['ingress']['nap']
        else:
            ingress_nap = '/'.join([self.wtapi_ledger[virtual_link_uuid]['ingress']['nap'], '32'])
        ingress_sip_uuid = self.wtapi_ledger[virtual_link_uuid]['ingress']['sip']
        if len(self.wtapi_ledger[virtual_link_uuid]['egress']['nap'].split('/')) == 2:
            egress_nap = self.wtapi_ledger[virtual_link_uuid]['egress']['nap']
        else:
            egress_nap = '/'.join([self.wtapi_ledger[virtual_link_uuid]['egress']['nap'], '32'])
        egress_sip_uuid = self.wtapi_ledger[virtual_link_uuid]['egress']['sip']
        if 'qos' in self.wtapi_ledger[virtual_link_uuid] and self.wtapi_ledger[virtual_link_uuid]['qos']:
            if 'minimum_bandwidth' in self.wtapi_ledger[virtual_link_uuid]['qos']:
                requested_capacity = float(self.wtapi_ledger[virtual_link_uuid]['qos']['minimum_bandwidth']['bandwidth']) * 1e6
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
        LOG.debug(f'Parameters for VL_CREATE: wim={wim_host}, ingress_nap={ingress_nap}, ingress_sip={ingress_sip_uuid}, '
                  f'egress_nap={egress_nap}, egress_sip={egress_sip_uuid}, '
                  f'requested_capacity={requested_capacity}, requested_latency={requested_latency}')
        self.wtapi_ledger[virtual_link_uuid]['active_connectivity_services'] = []
        if self.wtapi_ledger[virtual_link_uuid]['layer'] == 'MPLS':
            service_layer = 'MPLS'
            discovery_layer = 'MPLS_ARP'
        else:
            service_layer = 'ETH'
            discovery_layer = 'ARP'
        connectivity_services = [
            self.engine.generate_cs_from_nap_pair(
                ingress_nap, egress_nap,
                ingress_sip_uuid, egress_sip_uuid,
                layer=service_layer, direction='UNIDIRECTIONAL',
                requested_capacity=requested_capacity, latency=requested_latency),
            self.engine.generate_cs_from_nap_pair(
                egress_nap, ingress_nap,
                egress_sip_uuid, ingress_sip_uuid,
                layer=service_layer, direction='UNIDIRECTIONAL',
                requested_capacity=requested_capacity, latency=requested_latency),
            self.engine.generate_cs_from_nap_pair(
                ingress_nap, egress_nap,
                ingress_sip_uuid, egress_sip_uuid,
                layer=discovery_layer, direction='UNIDIRECTIONAL',
                requested_capacity=requested_capacity, latency=requested_latency),
            self.engine.generate_cs_from_nap_pair(
                egress_nap, ingress_nap,
                egress_sip_uuid, ingress_sip_uuid,
                layer=discovery_layer, direction='UNIDIRECTIONAL',
                requested_capacity=requested_capacity, latency=requested_latency),
        ]
        if self.wtapi_ledger[virtual_link_uuid]['router_flow_creation'] \
                and self.wtapi_ledger[virtual_link_uuid]['egress']['conf'].get('router_ext_ip') \
                and self.wtapi_ledger[virtual_link_uuid]['ingress']['type'] == 'endpoint':
            self.wtapi_ledger[virtual_link_uuid]['router_flow_operational'] = True
            router_connectivity_services = [
                self.engine.generate_cs_from_nap_pair(
                    ingress_nap, self.wtapi_ledger[virtual_link_uuid]['egress']['conf'].get('router_ext_ip'),
                    ingress_sip_uuid, egress_sip_uuid,
                    layer=service_layer, direction='UNIDIRECTIONAL'),
                self.engine.generate_cs_from_nap_pair(
                    self.wtapi_ledger[virtual_link_uuid]['egress']['conf'].get('router_ext_ip'), ingress_nap,
                    egress_sip_uuid, ingress_sip_uuid,
                    layer=service_layer, direction='UNIDIRECTIONAL'),
                self.engine.generate_cs_from_nap_pair(
                    ingress_nap, self.wtapi_ledger[virtual_link_uuid]['egress']['conf'].get('router_ext_ip'),
                    ingress_sip_uuid, egress_sip_uuid,
                    layer=discovery_layer, direction='UNIDIRECTIONAL'),
                self.engine.generate_cs_from_nap_pair(
                    self.wtapi_ledger[virtual_link_uuid]['egress']['conf'].get('router_ext_ip'), ingress_nap,
                    egress_sip_uuid, ingress_sip_uuid,
                    layer=discovery_layer, direction='UNIDIRECTIONAL'),
            ]
        elif self.wtapi_ledger[virtual_link_uuid]['router_flow_creation'] \
                and self.wtapi_ledger[virtual_link_uuid]['ingress']['conf'].get('router_ext_ip') \
                and self.wtapi_ledger[virtual_link_uuid]['egress']['type'] == 'endpoint':
            self.wtapi_ledger[virtual_link_uuid]['router_flow_operational'] = True
            router_connectivity_services = [
            self.engine.generate_cs_from_nap_pair(
                self.wtapi_ledger[virtual_link_uuid]['ingress']['conf'].get('router_ext_ip'), egress_nap,
                ingress_sip_uuid, egress_sip_uuid,
                layer=service_layer, direction='UNIDIRECTIONAL'),
            self.engine.generate_cs_from_nap_pair(
                egress_nap, self.wtapi_ledger[virtual_link_uuid]['ingress']['conf'].get('router_ext_ip'),
                egress_sip_uuid, ingress_sip_uuid,
                layer=service_layer, direction='UNIDIRECTIONAL'),
            self.engine.generate_cs_from_nap_pair(
                self.wtapi_ledger[virtual_link_uuid]['ingress']['conf'].get('router_ext_ip'), egress_nap,
                ingress_sip_uuid, egress_sip_uuid,
                layer=discovery_layer, direction='UNIDIRECTIONAL'),
            self.engine.generate_cs_from_nap_pair(
                egress_nap, self.wtapi_ledger[virtual_link_uuid]['ingress']['conf'].get('router_ext_ip'),
                egress_sip_uuid, ingress_sip_uuid,
                layer=discovery_layer, direction='UNIDIRECTIONAL'),
        ]
        elif self.wtapi_ledger[virtual_link_uuid]['router_flow_creation'] \
                and self.wtapi_ledger[virtual_link_uuid]['ingress']['conf'].get('router_ext_ip') \
                and self.wtapi_ledger[virtual_link_uuid]['egress']['conf'].get('router_ext_ip'):
            self.wtapi_ledger[virtual_link_uuid]['router_flow_operational'] = True
            LOG.warning(f'MSCS not implemented yet in the wrapper')
            # RouterA<->RouterB, VNFA<->RouterB RouterA<->VNFB, tests needed before implementing
            router_connectivity_services = []
            #     router_connectivity_services = [
            #     self.engine.generate_cs_from_nap_pair(
            #         self.wtapi_ledger[virtual_link_uuid]['ingress']['conf']['router_ext_ip'], egress_nap,
            #         ingress_sip_uuid, egress_sip_uuid,
            #         layer='MPLS', direction='UNIDIRECTIONAL'),
            #     self.engine.generate_cs_from_nap_pair(
            #         egress_nap, self.wtapi_ledger[virtual_link_uuid]['ingress']['conf']['router_ext_ip'],
            #         egress_sip_uuid, ingress_sip_uuid,
            #         layer='MPLS', direction='UNIDIRECTIONAL'),
            #     self.engine.generate_cs_from_nap_pair(
            #         self.wtapi_ledger[virtual_link_uuid]['ingress']['conf']['router_ext_ip'], egress_nap,
            #         ingress_sip_uuid, egress_sip_uuid,
            #         layer='MPLS_ARP', direction='UNIDIRECTIONAL'),
            #     self.engine.generate_cs_from_nap_pair(
            #         egress_nap, self.wtapi_ledger[virtual_link_uuid]['ingress']['conf']['router_ext_ip'],
            #         egress_sip_uuid, ingress_sip_uuid,
            #         layer='MPLS_ARP', direction='UNIDIRECTIONAL'),
            # ]
        else:
            router_connectivity_services = []

        for connectivity_service in router_connectivity_services:
            try:
                self.engine.create_connectivity_service(wim_host, connectivity_service)
                LOG.debug(f'router_registry: {self.aux_wtapi_ledger["router_cs_registry"]}')
                if self.wtapi_ledger[virtual_link_uuid]['ingress']['type'] == 'endpoint' \
                        and self.wtapi_ledger[virtual_link_uuid]['router_flow_operational']:
                    idx = next((index for (index, d) in enumerate(self.aux_wtapi_ledger['router_cs_registry'])
                                if d["client_endpoint_uuid"] == self.wtapi_ledger[virtual_link_uuid]['ingress']['location']
                                and d["pop_uuid"] == self.wtapi_ledger[virtual_link_uuid]['egress']['location']), None)
                    self.aux_wtapi_ledger['router_cs_registry'][idx]['routing_cs'].append(connectivity_service['uuid'])
                elif self.wtapi_ledger[virtual_link_uuid]['egress']['type'] == 'endpoint' \
                        and self.wtapi_ledger[virtual_link_uuid]['router_flow_operational']:
                    idx = next((index for (index, d) in enumerate(self.aux_wtapi_ledger['router_cs_registry'])
                                if d["client_endpoint_uuid"] == self.wtapi_ledger[virtual_link_uuid]['egress']['location']
                                and d["pop_uuid"] == self.wtapi_ledger[virtual_link_uuid]['ingress']['location']), None)
                    self.aux_wtapi_ledger['router_cs_registry'][idx]['routing_cs'].append(connectivity_service['uuid'])
                else:
                    # TODO: this is mscs flow
                    pass

            except Exception as exc:
                tb = "".join(traceback.format_exc().split("\n"))
                LOG.error(f'{connectivity_service["uuid"]} generated an exception: {tb}')

        for connectivity_service in connectivity_services:
            try:
                self.engine.create_connectivity_service(wim_host, connectivity_service)
                self.wtapi_ledger[virtual_link_uuid]['active_connectivity_services'].append(
                    connectivity_service['uuid'])
                if self.wtapi_ledger[virtual_link_uuid]['ingress']['type'] == 'endpoint' \
                        and self.wtapi_ledger[virtual_link_uuid]['router_flow_operational']:
                    idx = next((index for (index, d) in enumerate(self.aux_wtapi_ledger['router_cs_registry'])
                                if d["client_endpoint_uuid"] == self.wtapi_ledger[virtual_link_uuid]['ingress']['location']
                                and d["pop_uuid"] == self.wtapi_ledger[virtual_link_uuid]['egress']['location']), None)
                    self.aux_wtapi_ledger['router_cs_registry'][idx]['associated_cs'].append(connectivity_service['uuid'])
                elif self.wtapi_ledger[virtual_link_uuid]['egress']['type'] == 'endpoint' \
                        and self.wtapi_ledger[virtual_link_uuid]['router_flow_operational']:
                    idx = next((index for (index, d) in enumerate(self.aux_wtapi_ledger['router_cs_registry'])
                                if d["client_endpoint_uuid"] == self.wtapi_ledger[virtual_link_uuid]['egress']['location']
                                and d["pop_uuid"] == self.wtapi_ledger[virtual_link_uuid]['ingress']['location']), None)
                    self.aux_wtapi_ledger['router_cs_registry'][idx]['associated_cs'].append(connectivity_service['uuid'])
                else:
                    # TODO: this is mscs flow
                    pass


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
        LOG.debug(f'Network Service {self.wtapi_ledger[virtual_link_uuid]["service_uuid"]}: Removing virtual links')
        # wim_host = self.wtapi_ledger[virtual_link_uuid]['wim']['host']
        conn_services_to_remove = []
        # Gather all connectivity services
        # if 'active_connectivity_services' in self.wtapi_ledger[virtual_link_uuid].keys():
        #     conn_services_to_remove = [
        #         {'cs_uuid': cs_uuid, 'vl_uuid': virtual_link_uuid}
        #         for cs_uuid in self.wtapi_ledger[virtual_link_uuid]['active_connectivity_services']
        #     ]
        # else:
        #
        #     LOG.warning(f'Requested virtual_links_remove for {self.wtapi_ledger[virtual_link_uuid]["vl_id"]} '
        #                 f'but there are no active connections')

        for rel_virtual_link_id in self.wtapi_ledger[virtual_link_uuid]['related_cs_sets']:
            if 'active_connectivity_services' in self.wtapi_ledger[rel_virtual_link_id].keys():
                conn_services_to_remove.extend(
                    [
                        {
                            'cs_uuid': cs_uuid,
                            'vl_uuid': rel_virtual_link_id,
                            'wim_host': self.wtapi_ledger[rel_virtual_link_id]['wim']['host']
                        }
                        for cs_uuid in self.wtapi_ledger[rel_virtual_link_id]['active_connectivity_services']
                    ]
                )
            else:
                LOG.warning(f'Requested virtual_links_remove for {rel_virtual_link_id} '
                            f'but there are no active connections')

        # Take all connectivity services down
        vl_removed = set()
        for cs in conn_services_to_remove:
            try:
                self.engine.remove_connectivity_service(cs['wim_host'], cs['cs_uuid'])
                LOG.debug(f'cs: {cs}; router_cs_registry: {self.aux_wtapi_ledger["router_cs_registry"]}')
                vl_removed.update([cs['vl_uuid']])
                # If it was associated with a router aggregation, remove linkage
                if self.wtapi_ledger[cs['vl_uuid']]['ingress']['type'] == 'endpoint' \
                        and self.wtapi_ledger[cs['vl_uuid']]['router_flow_operational']:
                    router_aggreg_idx = next((index for (index, d) in enumerate(self.aux_wtapi_ledger['router_cs_registry'])
                                if d["client_endpoint_uuid"] == self.wtapi_ledger[cs['vl_uuid']]['ingress']['location']
                                and d["pop_uuid"] == self.wtapi_ledger[cs['vl_uuid']]['egress']['location']), None)
                    del self.aux_wtapi_ledger['router_cs_registry'][router_aggreg_idx]['associated_cs'][
                        self.aux_wtapi_ledger['router_cs_registry'][router_aggreg_idx]['associated_cs'].index(cs['cs_uuid'])]
                    if not self.aux_wtapi_ledger['router_cs_registry'][router_aggreg_idx]['associated_cs']:
                        for router_cs_uuid in self.aux_wtapi_ledger['router_cs_registry'][router_aggreg_idx]['routing_cs']:
                            self.engine.remove_connectivity_service(cs['wim_host'], router_cs_uuid)
                        del self.aux_wtapi_ledger['router_cs_registry'][router_aggreg_idx]
                elif self.wtapi_ledger[cs['vl_uuid']]['egress']['type'] == 'endpoint' \
                        and self.wtapi_ledger[cs['vl_uuid']]['router_flow_operational']:
                    router_aggreg_idx = next((index for (index, d) in enumerate(self.aux_wtapi_ledger['router_cs_registry'])
                                if d["client_endpoint_uuid"] == self.wtapi_ledger[cs['vl_uuid']]['egress']['location']
                                and d["pop_uuid"] == self.wtapi_ledger[cs['vl_uuid']]['ingress']['location']), None)
                    del self.aux_wtapi_ledger['router_cs_registry'][router_aggreg_idx]['associated_cs'][
                        self.aux_wtapi_ledger['router_cs_registry'][router_aggreg_idx]['associated_cs'].index(cs['cs_uuid'])]
                    if not self.aux_wtapi_ledger['router_cs_registry'][router_aggreg_idx]['associated_cs']:
                        for router_cs_uuid in self.aux_wtapi_ledger['router_cs_registry'][router_aggreg_idx]['routing_cs']:
                            self.engine.remove_connectivity_service(cs['wim_host'], router_cs_uuid)
                        del self.aux_wtapi_ledger['router_cs_registry'][router_aggreg_idx]
                else:
                    # TODO: this is mscs flow
                    pass
            except Exception as exc:
                tb = "".join(traceback.format_exc().split("\n"))
                LOG.error(f'{cs["cs_uuid"]} generated an exception: {tb}')



        for vl_uuid in vl_removed:
            self.wtapi_ledger[vl_uuid]['active_connectivity_services'] = []

        LOG.debug(f'Virtual Links removed: {vl_removed}')
        # TODO: remove router link references if its router_type and check if router cs has to be removed

        # for cs in conn_services_to_remove:
        #     self.engine.remove_connectivity_service(wim_host, cs['cs_uuid'])
        # self.wtapi_ledger[virtual_link_uuid]['active_connectivity_services'] = []

        return {'result': True, 'message': vl_removed, 'error': None}

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
        def send_error_response(error, virtual_link_uuid):

            response = {
                'message': error,
                'request_status': 'ERROR'
            }
            msg = ' Response on create request: ' + str(response)
            LOG.info('Virtual link ' + str(virtual_link_uuid) + msg)
            self.manoconn.notify(topics.WAN_CONFIGURE,
                                 yaml.dump(response),
                                 correlation_id=properties.correlation_id)

        LOG.debug(f'CONF_LEDGER_DEBUG: {self.wtapi_ledger}')
        LOG.debug(f'CONF_AUX_LEDGER_DEBUG: {self.aux_wtapi_ledger}')

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

        # TODO: check if router_ext_ip: A.B.C.D connection is necesary for this endpoint (after endpoints info)

        # self.aux_wtapi_ledger['router_cs_registry']
        # Schedule the tasks that the Wrapper should do for this request.
        schedule = [
            'get_wim_info',
            'get_endpoints_info',
            'match_endpoints_with_sips',
            'check_router_connection',
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
                'router_flow_creation': False,
                'router_flow_operational': False,
                'schedule': schedule,
                'topic': properties.reply_to,
            }
        else:
            raise KeyError('Duplicated virtual_link_uuid')

        if message['egress']['location'] == message['ingress']['location']:
            LOG.info(f"NS {service_instance_id}, VL:{virtual_link_id}: "
                     f"egress {message['egress']['location']} AND "
                     f"ingress {message['ingress']['location']} are the same endpoint, aborting")
            self.respond_to_request(virtual_link_uuid)
        else:
            # Start the chain of tasks
            msg = "New virtual link request received. Creating flows..."
            LOG.info(f"NS {service_instance_id}, VL:{virtual_link_id}: {msg}")
            self.start_next_task(virtual_link_uuid)

        return self.wtapi_ledger[virtual_link_uuid]['schedule']

    def wan_network_deconfigure(self, ch, method, properties, payload):
        """
        This function handles a received message on the *.service.wan.deconfigure
        topic.
        payload: { service_instance_id: :uuid:}
        """
        def send_error_response(error, virtual_link_uuid):

            response = {
                'message': error,
                'request_status': 'ERROR'
            }
            msg = ' Response on remove request: ' + str(response)
            LOG.info('Service ' + str(virtual_link_uuid) + msg)
            self.manoconn.notify(topics.WAN_DECONFIGURE,
                                 yaml.dump(response),
                                 correlation_id=properties.correlation_id)

        LOG.debug(f'DECONF_LEDGER_DEBUG: {self.wtapi_ledger}')
        LOG.debug(f'DECONF_AUX_LEDGER_DEBUG: {self.aux_wtapi_ledger}')

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

        virtual_link_uuid_list = [
            self.wtapi_ledger[virtual_link]['uuid'] for virtual_link in self.wtapi_ledger.keys()
            if self.wtapi_ledger[virtual_link]['vl_id'] == message['vl_id'] and
                self.wtapi_ledger[virtual_link]['service_uuid'] == service_instance_id
        ]
        try:
            virtual_link_uuid = virtual_link_uuid_list[0]
            self.wtapi_ledger[virtual_link_uuid]['related_cs_sets'] = virtual_link_uuid_list
        except Exception as e:
            send_error_response(e, None)
            return

        # LOG.debug(f"Virtual links deployed {virtual_links} for service {service_instance_id}")
        add_schedule = [
            'virtual_links_remove',
            'delete_reference_database',
            'respond_to_request',
        ]
        for vl in virtual_link_uuid_list:
            self.wtapi_ledger[vl]['status'] = 'TERMINATING'
        self.wtapi_ledger[virtual_link_uuid]['schedule'].extend(add_schedule)
        self.wtapi_ledger[virtual_link_uuid]['topic'] = properties.reply_to
        self.wtapi_ledger[virtual_link_uuid]['orig_corr_id'] = properties.correlation_id

        msg = "Virtual link remove request received."
        LOG.info("Network Service {}: {}".format(service_instance_id, msg))
        # Start the chain of tasks
        self.start_next_task(virtual_link_uuid)

        return

    def wan_list_capabilites(self, ch, method, properties, payload):
        """
        Retrieve a list of the WIMs registered to the IA.

        topic: infrastructure.management.wan.list
        data: null
        return: [{uuid: String, name: String, attached_vims: [Strings], attached_endpoints: [Strings]}, qos: [{node_1: String, node_2: String, latency: int: latency_unit: String, bandwidth: int, bandwidth_unit: String}]], when exist an error, request_status is "ERROR", message field carries a string with the error message and the other fields are empty.


        :param ch:
        :param method:
        :param properties:
        :param payload:
        :return:
        """
        def send_error_response(error, virtual_link_uuid):

            response = {
                'message': error,
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

        LOG.info('infrastructure.tapi.management.wan.list message received')
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

    def respond_to_request(self, virtual_link_uuid):
        """
        This method creates a response message for the sender of requests.
        """
        if self.wtapi_ledger[virtual_link_uuid]['error'] is None \
                and self.wtapi_ledger[virtual_link_uuid]['message'] is None:
            message = {
                # 'message': f'Virtual link {self.wtapi_ledger[virtual_link_uuid]["vl_id"]} created',
                'message': None,
                'request_status': 'COMPLETED'
            }
        elif self.wtapi_ledger[virtual_link_uuid]['error'] is None \
                and self.wtapi_ledger[virtual_link_uuid]['message'] is not None:
            message = {
                # 'message': self.wtapi_ledger[virtual_link_uuid]['message'],
                'message': None,
                'request_status': 'COMPLETED'
            }
        elif self.wtapi_ledger[virtual_link_uuid]['error'] is not None \
                and self.wtapi_ledger[virtual_link_uuid]['message'] is not None:
            message = {
                'message': self.wtapi_ledger[virtual_link_uuid]['message'],
                'request_status': 'ERROR'
            }
        else:
            message = {
                'message': None,
                'request_status': 'ERROR'
            }

        LOG.info("Generating response to the workflow request for {}".format(virtual_link_uuid))

        corr_id = self.wtapi_ledger[virtual_link_uuid]['orig_corr_id']
        topic = self.wtapi_ledger[virtual_link_uuid]['topic']
        # message["timestamp"] = time.time()
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

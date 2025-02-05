# check_configuration_for_hosts_accessibility - This script verifies whether hosts defined in a mesh configuration
#                                               are resolvable via DNS. It is designed to scan all the hosts in the
#                                               provided mesh configuration, which is loaded using the psconfig API,
#                                               and checks if the hosts can be resolved by DNS queries. This can be
#                                               used to identify hosts that might have gone offline or have invalid DNS
#                                               records.
#
#                                               The process iterates through all hosts specified in each configuration,
#                                               checks their DNS resolution status, and generates a list of hosts that
#                                               are no longer resolvable and configurations where they
#                                               still can be found. The output is a list of these hosts, indicating
#                                               that they should be updated or removed from the configuration.
#
# Author: Yana Holoborodko
# Copyright 2024
import socket
import psconfig.api
import requests
import hashlib
import urllib3
from alarms import alarms
from datetime import datetime
from pymemcache.client import base


def host_resolvable(host):
    """
    Checks whether the host is resolvable via DNS for either IPv4 or IPv6 addresses.
    """
    try:
        addr_info = socket.getaddrinfo(host, None)
        if addr_info:
            # print(f"{host} is resolvable via DNS")
            return True
    except socket.gaierror:
        # print(f"{host} is NOT resolvable via DNS")
        pass
    return False

def extract_configs_from_url(mesh_url):
    """
    Fetches and extracts the configuration map from a mesh configuration URL.
    """
    try:
        response = requests.get(mesh_url)
        response.raise_for_status()
        mesh_config = psconfig.api.PSConfig(mesh_url)
        return extract_configs(mesh_config)
    except requests.exceptions.RequestException as e:
        print(f"Error fetching mesh configuration from {mesh_url}: {e}")
        return {}

def extract_configs(mesh_config):
    """
    Extracts the configuration map from the mesh configuration.
    """
    return mesh_config.get_config_host_map()

def check_configuration_for_hosts_accessibility(config, all_conf_hosts):
    """
    Iterates through all hosts in a given configuration and checks if they are resolvable via DNS.
    Returns a list of hosts that are not resolvable and need to be updated.
    """
    hosts_to_update = []
    for h in all_conf_hosts:
        if not host_resolvable(h):
            hosts_to_update.append(h)
    msg = '\n'.join(hosts_to_update) if hosts_to_update else "All hosts are resolvable"
    # print("------------------------------------------------------")
    return f"Information about following hosts in {config} configuration must be updated:\n{msg}", hosts_to_update

def main():
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    mesh_url = "https://psconfig.aglt2.org/pub/config"
    configs = extract_configs_from_url(mesh_url)
    alarmType = 'unresolvable host'
    # create the alarm objects
    alarmOnHost = alarms("Networking", 'Infrastructure', alarmType)

    client = base.Client(('memcached.collectors', 11211))

    if not configs:
        print("No configurations found or failed to load the mesh configuration.")
        return

    inaccessible_hosts = {}
    for config, hosts in configs.items():
        # print("\n******************************************************")
        result_message, hosts_to_remove = check_configuration_for_hosts_accessibility(config, hosts)
        # print(result_message)
        for host in hosts_to_remove:
            if host not in inaccessible_hosts:
                inaccessible_hosts[host] = []
            inaccessible_hosts[host].append(config)
        # print("******************************************************\n")

    if inaccessible_hosts:
        # print("\nSummary:")
        for host, host_configs in inaccessible_hosts.items():

            netsite_bytes = client.get(f'netsite_{host}')
            site = ''
            if netsite_bytes:
                site = netsite_bytes.decode('utf-8')
                # print('netsite', site)
            else:
                rcsite_bytes = client.get(f'rcsite_{host}')
                if netsite_bytes:
                    site = rcsite_bytes.decode('utf-8')

            doc = {
                'host': host,
                'site': site,
                'configurations': host_configs,
            }
            current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            toHash = ','.join([host] + host_configs + [current_datetime])
            doc['alarm_id'] = hashlib.sha224(toHash.encode('utf-8')).hexdigest()

            alarmOnHost.addAlarm(body=alarmType, tags=[host], source=doc)
            print(f"Host '{host}' at {site} needs updates in configurations: {', '.join(host_configs)}")
    else:
        print("All hosts are resolvable. No updates needed.")
    return inaccessible_hosts

if __name__ == '__main__':
    hosts = main()

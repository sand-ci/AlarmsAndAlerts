import requests
import requests.exceptions
import socket
import json
import logging
import argparse
from elasticsearch import Elasticsearch, NotFoundError  # Import NotFoundError
from elasticsearch.helpers import scan
from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

def get_members_for_type(entry):
    members = set()
    if 'members' in entry['members'].keys():
        for h in entry['members']['members']:
            members.add(h)
    if 'a_members' in entry['members'].keys():
        for h in entry['members']['a_members']:
            members.add(h)
    if 'b_members' in entry['members'].keys():
        for h in entry['members']['b_members']:
            members.add(h)
    return members

def has_event(entry, event):
    for ev in entry['event-types']:
        if ev['event-type'] == event:
            return True
    return False

def get_members(mesh_config, event_type):
    event_type_map = {'packet-trace': 'traceroute',
                      'packet-loss-rate': 'perfsonarbuoy/owamp',
                      'histogram-owdelay': 'perfsonarbuoy/owamp',
                      'throughput': 'perfsonarbuoy/bwctl'}
    found = False
    mesh_c = json.loads(mesh_config)
    for entry in mesh_c.get('tests', []):
        try:
            if 'parameters' in entry and entry['parameters']['type'] == event_type_map[event_type]:
                found = True
                yield entry['description'], get_members_for_type(entry)
        except KeyError:
            # Handle the case where 'parameters' or 'type' key is not present in the entry
            pass
    if not found:
        print("No mesh members found for event type: %s" % event_type)

log = logging.getLogger("nap")

def test_esmond_meta(args, hostname):
    if not (hostname or args.hostcert or args.hostkey or args.events):
        print("Missing plugin arguments")
        return 'CRITICAL'

    if not args.autourl:
        print("Missing auto-url argument")
        return 'CRITICAL'

    host = socket.gethostbyname(hostname)  # force IPv4

    if args.esmond:
        url = args.esmond + '/esmond/perfsonar/archive/?format=json&input-source=' + hostname + \
             '&time-range=' + str(args.timerange)
    else:
        url = host + '/esmond/perfsonar/archive/?format=json&input-source=' + hostname + \
             '&time-range=' + str(args.timerange)
    if args.reverse:
        url = host + '/esmond/perfsonar/archive/?format=json&destination=' + hostname + \
              '&time-range=' + str(args.timerange)
    if args.esmond:
        url = url + '&measurement-agent=' + hostname

    for event in args.events.split(','):
        url = url + '&event-type=' + event

    log.debug(url)
    req_error = None
    try:
        esmond_meta = requests.get("http://" + url, timeout=args.req_timeout, headers={'Host': hostname})
        esmond_meta.raise_for_status()
    except requests.exceptions.RequestException as e:
        req_error = str(e)

    if req_error:  # https request as fallback
        try:
            esmond_meta = requests.get("https://" + url, timeout=args.req_timeout, headers={'Host': hostname},
                                    verify=False)
            esmond_meta.raise_for_status()
        except requests.exceptions.RequestException as e:
            req_error = str(e)
        else:
            req_error = None

    if req_error:
        print("Connection failed (%s)" % req_error)
        return 'CRITICAL'

    mesh_url = urljoin(args.autourl, hostname)
    try:
        meshes = requests.get(mesh_url+'?format=meshconfig', timeout=args.req_timeout)
        meshes.raise_for_status()
    except requests.exceptions.RequestException as e:
        print("Failed to get mesh from %s" % mesh_url)
        return 'CRITICAL'

    try:
        esmond_metadata = json.loads(esmond_meta.text)
    except Exception as e:
        print("Unable to parse esmond metadata")
        return 'CRITICAL'

    missing = {}
    found = set()
    missing_entries = False
    for event in args.events.split(','):
        for mesh, ev_mem in dict(get_members(meshes.text, event)).items():
            if not ev_mem:
                print("No members for a particular mesh %s and event type: %s" % (mesh, event))
                return 'CRITICAL'
            if hostname in ev_mem:
                ev_mem.remove(hostname)
            mem_to_check = ev_mem.copy()
            for member in ev_mem:
                for entry in esmond_metadata:
                    if (not args.reverse) and entry['input-destination'] == member and has_event(entry, event) \
                                                  and member in mem_to_check:
                        if 'IPv4' in mesh and ':' not in entry['destination']:
                            mem_to_check.remove(member)
                        elif 'IPv6' in mesh:
                            if ':' in entry['destination']:
                                mem_to_check.remove(member)
                            else:
                                host_addr = socket.getaddrinfo(member, 80, 0, 0, socket.IPPROTO_TCP)
                                if not any(x[0] == socket.AF_INET6 for x in host_addr):
                                    mem_to_check.remove(member)
                        elif 'IPv4' not in mesh and 'IPv6' not in mesh:
                            mem_to_check.remove(member)
                    if args.reverse and entry['input-source'] == member and has_event(entry, event) \
                            and member in mem_to_check:
                        if 'IPv4' in mesh and ':' not in entry['source']:
                            mem_to_check.remove(member)
                        elif 'IPv6' in mesh and ':' in entry['source']:
                            mem_to_check.remove(member)
                        elif 'IPv4' not in mesh and 'IPv6' not in mesh:
                            mem_to_check.remove(member)
            if mem_to_check:
                missing[mesh + ' (' + event + ')'] = mem_to_check
                missing_entries = True
                found.update(ev_mem - mem_to_check)
            else:
                found.update(ev_mem)

    if missing_entries:
        missing_count = set()
        for v in missing.values():
            missing_count.update(v)
        comp_rate = round(
                    float(len(found)) / (len(found) + len(missing_count)),
                    4) * 100
        events = args.events.split(',')
        if 'packet-trace' in events and comp_rate >= 90:
            return 'OK'
        elif 'throughput' in events and comp_rate >= 70:
            return 'OK'
        elif 'histogram-owdelay' in events and comp_rate >= 80:
            return 'OK'
        else:
            return 'WARNING' if comp_rate <= 20 else 'CRITICAL'

    return 'OK'

if __name__ == '__main__':
    import sys
    from elasticsearch import Elasticsearch
    from elasticsearch.helpers import scan
    from urllib.parse import urljoin

    sys.argv = [
        "check_ps_es",
        "--hostcert", "/etc/grid-security/host/cert.pem",
        "--hostkey", "/etc/grid-security/host/key.pem",
        "-T", "60000",
        "-e", "throughput",
        "-r", "864000",
        "-R",
        "-M", "https://psconfig.aglt2.org/pub/auto/"
    ]

    parser = argparse.ArgumentParser(description="Checks metadata in esmond wrt. tests configured in the mesh config")
    parser.add_argument('--hostcert', help='Path to hostcert.pem')
    parser.add_argument('--hostkey', help='Path to hostkey.pem')
    parser.add_argument('-T', '--requests-timeout', dest="req_timeout", default=60, type=int,
                        help='Timeout for HTTP(s) requests')
    parser.add_argument('-e', '--events', help='Comma separated list of events to query')
    parser.add_argument('-r', '--timerange', default=7200, type=int, help='Time interval for esmond query')
    parser.add_argument('-R', '--reverse', action='store_true', default=False, help='Check reverse direction')
    parser.add_argument('-M', '--host-autourl', help="Mesh config auto-URL", dest="autourl")
    parser.add_argument('--esmond', help='Specify explicit esmond location (to be used instead of hostname/esmond)')

    args = parser.parse_args()

    user, passwd, mapboxtoken = None, None, None

    with open("creds.key") as f:
        user = f.readline().strip()
        passwd = f.readline().strip()
        mapboxtoken = f.readline().strip()

    def ConnectES():
        global user, passwd

        es = None

        try:
            credentials = (user, passwd)
            es = Elasticsearch(
                [{'host': 'atlas-kibana.mwt2.org', 'port': 9200, 'scheme': 'https'}],
                request_timeout=240, basic_auth=credentials, max_retries=10
            )
            success = es.ping()
            print('Success' if success else 'Fail')
        except Exception as error:
            print(">>>>>> Elasticsearch Client Error:", error)

        return es

    es = ConnectES()

    if es:
        try:
            hosts = scan(client=es, query={"query": {"match_all": {}}}, scroll='1m', raise_on_error=False)
        except Exception as e:
            print(f"Error querying hosts from Elasticsearch: {e}")
        else:
            try:
                for host in hosts:
                    host_name = host['_source'].get('src_host')

                    if host_name:
                        try:
                            status = test_esmond_meta(args, host_name)
                        except socket.gaierror as e:
                            print(f"Error resolving hostname {host_name}: {e}")
                            continue  # Skip to the next iteration of the loop

                        src_site = host['_source'].get('src_site')
                        print(f"Host: {host_name}, Status: {status}, Src Site: {src_site}")

            except NotFoundError as e:  # Catch NotFoundError
                print(f"NotFoundError: {e}")
            finally:
                # Clear the scroll context by iterating over the generator
                for _ in hosts:
                    pass
                print("Complete")

    else:
        print("Elasticsearch connection failed. Cannot proceed.")
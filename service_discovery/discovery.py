'''
discover services
'''
import os
import subprocess
import re
import requests
import psutil
from config_handler import configurator
from common.util import *

logger = expoter_logging(COLLECTD_MGR)
JCMD_PID_DICT = dict()
SERVICE_NAME = {
    "elasticsearch": "ES",
    "apache": "apache",
    "tomcat": "tomcat",
    "haproxy": "haproxy",
    "mysql": "mysql",
    "mssql": "mssql",
    "postgres": "postgres",
    "nginx": "nginx",
    "tpcc": "tpcc",
    "kafka.Kafka": "kafka",
    "zookeeper": "zookeeper",
    "hxconnect": "hxconnect",
    "cassandra": "cassandra",
    "esalogstore": "ESAlogstore",
    #"knox": "knox",
    "redis": "redis",
    "OOZIE": "OOZIE",
    "YARN": "YARN",
    "HDFS": "HDFS",
    "SPARK2": "SPARK2"
}
SERVICES = [
    "elasticsearch",
    "apache",
    "tomcat",
    "haproxy",
    "redis",
    "mysql",
    "mssql",
    "postgres",
    "nginx",
    "tpcc",
    "kafka.Kafka",
    "zookeeper",
    "hxconnect",
    "cassandra",
    "esalogstore",
    #"knox",
]
'''
Mapping for services and the plugin to be configured for them.
'''
SERVICE_PLUGIN_MAPPING = {
    "elasticsearch": "elasticsearch",
    "apache": "apache",
    "tomcat": "tomcat",
    "haproxy": "haproxy",
    "redis": "redisdb",
    "mysql": "mysql",
    "mssql": "mssql",
    "postgres": "postgres",
    "nginx": "nginx",
    "tpcc": "tpcc",
    "kafka.Kafka": "kafkatopic",
    "zookeeper": "zookeeperjmx",
    "hxconnect": "hxconnect",
    "cassandra": "cassandra",
    "OOZIE": "oozie",
    "YARN": "yarn",
    "HDFS": "namenode",
    "SPARK2": "spark"
}

POLLER_PLUGIN = ["elasticsearch"]
HADOOP_SERVICES = [
    "OOZIE",
    "YARN",
    "HDFS",
    "SPARK2"
]
HADOOP_SERVICE = {
    "yarn-rm-log": { \
         "service-name": "org.apache.hadoop.yarn.server.resourcemanager.ResourceManager",
         "service-list": ["yarn-rm", "yarn-audit"]
                   },
    "yarn-timeline-server": { \
         "service-name": "org.apache.hadoop.yarn.server.applicationhistoryservice.ApplicationHistoryServer",
         "service-list": ["yarn-timeline"]
                            },
    "hdfs-namenode": { \
         "service-name": "org.apache.hadoop.hdfs.server.namenode.NameNode",
         "service-list": ["hdfs-namenode", "hdfs-audit", "hdfs-gc", "hdfs-zkfc-manager"]
                     },
    "hdfs-journalnode": { \
         "service-name": "org.apache.hadoop.hdfs.qjournal.server.JournalNode",
         "service-list": ["hdfs-journalnode", "hdfs-gc", "hdfs-journalnode-manager"]
                        },
    "oozie-server": { \
         "service-name": "org.apache.catalina.startup.Bootstrap",
         "service-list": ["oozie-ops", "oozie-audit", "oozie-error-logs", "oozie-logs", "oozie-instrumentation", "oozie-jpa"],
         "service-cmd-line": "oozie-server"
                    },
    "hdfs-datanode": { \
    "service-name": "org.apache.hadoop.hdfs.server.datanode.DataNode",
    "service-list": ["hdfs-datanode"]
     }
}

def add_pid_usage(pid, pid_list):
    """Add usage stats of each pids"""

    pid_detail = psutil.Process(pid)
    if not pid_detail.is_running():
        return
    pid_info = {}
    pid_info["user"] = pid_detail.username()
    pid_info["process_id"] = pid
    pid_info["cpuUsage"] = pid_detail.cpu_percent()
    pid_info["memUsage"] = pid_detail.memory_percent()
    pid_info["status"] = "running"
    pid_list.append(pid_info)

def is_service_name(pid, service_cmd_line):
    """Check if PID is for oozie_server"""
    pid_detail = psutil.Process(pid)
    if re.search(service_cmd_line, str(pid_detail.cmdline())):
        return True
    return False

def check_jmx_enabled(pid):
    """Check if jmx enabled for java process"""
    pid_detail = psutil.Process(pid)
    if re.search("Dcom.sun.management.jmxremote", str(pid_detail.cmdline())):
        return True
    return False
def exec_subprocess(cmd):
    """ execute subprocess cmd """
    cmd_output = subprocess.Popen(
        cmd,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)
    res, err = cmd_output.communicate()
    return res

def parser_jcmd(service):
    """ Parser for jcmd """
    pid_list = list()
    try:
        if not JCMD_PID_DICT:
            java_avail = subprocess.check_call(
                ["java", "-version"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)
            if java_avail:
                return JCMD_PID_DICT

            #print( exec_subprocess("sudo jcmd | awk '{print $1 \" \" $2}'"))
            res = exec_subprocess("sudo jcmd | awk '{print $1 \" \" $2}'")
            if not res:
                return pid_list

            for line in res.splitlines():
                if not line:
                    continue
                out_list = line.split()
                if len(out_list) > 1:
                    JCMD_PID_DICT[out_list[1]] = int(out_list[0])

        for service_name, pid in JCMD_PID_DICT.items():
            if re.search(service, service_name):
                pid_list.append(pid)
        #print("{} pid list {}".format(service, pid_list))
        return pid_list
    except:
        logger.error("JCMD parser error")
        return pid_list

def get_hadoop_running_service_list():
    '''
     get hadoop services running in the client machine
    '''
    hadoop_running_service_list = list()
    for name, service_name in HADOOP_SERVICE.items():
        pid = parser_jcmd(service_name['service-name'])
        if not pid:
            continue

        if 'service-cmd-line' in service_name.keys():
            if is_service_name(pid[0], service_name['service-cmd-line']):
                hadoop_running_service_list.append(name)
            continue
        hadoop_running_service_list.append(name)
    return hadoop_running_service_list

def get_process_id(service):
    '''
    :param service: name of the service
    :return: return a list of PID's assosciated with the service along with their
    status, memUsage, cpuUsage and user
    '''
    logger.info("Get process id for service %s", service)
    pids = []

    if service in ["kafka.Kafka", "zookeeper"]:
        """if servcie == "tomcat":
          service = "apache"
        """

        pid_list = parser_jcmd(service)

        for pid in pid_list:
            if check_jmx_enabled(pid):
                add_pid_usage(pid, pids)

        logger.info("PIDs %s", pids)
        return pids

    if service == "apache":
        service = "httpd"
        out = exec_subprocess("lsb_release -d")
        for line in out.splitlines():
            if "Ubuntu" in line:
                service = "apache2"
                break

    try:
        process_id = ""
        for proc in psutil.process_iter(attrs=['pid', 'name', 'username', 'cmdline']):
            # Java processes
            if service in ["elasticsearch", "cassandra"]:
                if proc.info.get("name") == "java" and proc.info.get(
                        "username") == service:
                    process_id = proc.info.get("pid")
                    break

            elif service in ["tomcat"]:
                if proc.info.get("name") == "java" and "org.apache.catalina.startup.Bootstrap" in proc.info.get(
                        "cmdline"):
                    process_id = proc.info.get("pid")
                    break

            # Postgres process
            elif service in ["postgres"]:
                if proc.info.get("name") == "postmaster" or proc.info.get(
                        "name") == "postgres":
                    process_id = proc.info.get("pid")
                    break
            # Non java processes
            elif service in str(proc.info.get("name")):
                process_id = proc.info.get("pid")
                break

        add_pid_usage(process_id, pids)
        logger.info("PIDs %s", pids)
        return pids
    except BaseException:
        logger.info("PIDs %s", pids)
        return pids


def add_status(proc_dict):
    '''
    Find the status of the PID running, sleeping.
    :param dict: dictionary return by get_process_id
    :return: add status for the PID in th dictionary.
    '''
    # Add state, threads assosciated with the service PID
    fileobj = open('/proc/%d/status' % (proc_dict["PID"]))
    if not fileobj:
        return None
    lines = fileobj.readlines()
    for line in lines:
        if line.startswith("State:"):
            state = (line.split())[2]
            state = state.strip("()")
            proc_dict["state"] = state
        elif line.startswith("Threads:"):
            threads = line.split()
            threads = threads[1]
            proc_dict["threads"] = threads
    return proc_dict


def add_ports(service_dict, service):
    '''
    Add listening ports for the PID
    :param dict: dictionary returned by add_status
    :param service: name of the service
    :return: add listening ports for the PID to the dictionary
    '''
    logger.debug("Add ports %s %s", service_dict, service)
    ports = []
    if service == "apache":
        cmd = "netstat -anp | grep httpd"

        out = exec_subprocess("lsb_release -d")
        for line in out.splitlines():
            if "Ubuntu" in line:
                cmd = "netstat -anp | grep apache2"
                break
    else:
        cmd = "netstat -anp | grep %s" % (service_dict["PID"])

    out = exec_subprocess(cmd)
    for line in out.splitlines():
        line = line.split()
        if (line[5] == 'LISTEN') and (service == "apache" or str(service_dict['PID']) in line[6]):
            port = (line[3].split(':')[-1])
            if port not in ports:
                ports.append(port)
    service_dict['ports'] = ports
    return service_dict

def is_discover_service(service_name, discovered_service_list):
    """ is discivered services? """
    if SERVICE_NAME[service_name] in discovered_service_list:
        return True
    return False


def add_logger_config(service_dict, service):
    '''
    Add logger config
    '''
    service_dict["loggerConfig"] = []
    fluentd_plugins = configurator.get_fluentd_plugins_mapping().keys()
    for item in fluentd_plugins:
        if item.startswith(service.split(".")[0]):
            log_config = {}
            log_config["name"] = item
            log_config["config"] = {}
            log_config["config"]["filters"] = {}
            service_dict["loggerConfig"].append(log_config)
    return service_dict

def add_poller_config(service, service_dict):
    '''
    Add poller config
    '''
    service_dict["pollerConfig"] = {}
    poller_config = {}
    poller_config["config"] = {}

    for key, value in SERVICE_PLUGIN_MAPPING.items():
        if key == service:
            poller_config["name"] = value
            break

    config = configurator.get_metrics_plugins_params(poller_config["name"])
    for item in config["plugins"]:
        if item.get("config") and item.get("name") == poller_config["name"]:
            for item1 in item["config"]:
                poller_config["config"][item1["fieldName"]] = item1["defaultValue"]
    service_dict["pollerConfig"].update(poller_config)

    return service_dict


def add_agent_config(service, service_dict):
    '''
    Find the input config for the plugin fieldname:defaultvalue
    :param service: name of the service
    :param dict: poller_dict as the input
    :return:
    '''
    service_dict["agentConfig"] = {}
    agent_config = {}
    agent_config["config"] = {}
    for key, value in SERVICE_PLUGIN_MAPPING.items():
        if key == service:
            agent_config["name"] = value
            break
    #print service
    #print SERVICE_PLUGIN_MAPPING.keys()
    config = configurator.get_metrics_plugins_params(agent_config["name"])
    for item in config["plugins"]:
        if item.get("config") and item.get("name") == agent_config["name"]:
            # Config specific to jvm plugin
            if agent_config["name"] == "jvm":
                agent_config["config"]["process"] = service
                break
            if agent_config["name"] == "kafkatopic":
                agent_config["config"]["process"] = service
                for parameter in item["config"]:
                    agent_config["config"][parameter["fieldName"]] = parameter["defaultValue"]
                break
            for parameter in item["config"]:
                agent_config["config"][parameter["fieldName"]] = parameter["defaultValue"]

    # In apache plugin replace the port default value with the listening ports for apache/httpd,
    # if there are multiple listening ports for the PID assosciate the first
    # port with the PID
    if service == "apache":
        if len(service_dict["ports"]) != 0:
            agent_config["config"]["port"] = service_dict["ports"][0]
            if agent_config["config"]["port"] == "443":
                agent_config["config"]["secure"] = "true"

    service_dict["agentConfig"].update(agent_config)
    return service_dict

def check_nginx_plus():
    """ check nginx plus service  """
    logger.error('new in check condition')
    res = exec_subprocess("service nginx status")
    return res and 'Plus' in res.splitlines()[0]

def discover_services():
    '''
    Find the services which are running on the server and return it's PID list, users, CPUUsage,
    memUsage, Listening ports, input configuration for the plugin.
    :return:
    '''
    logger.info("Discover service started")
    discovery = {}
    for service in SERVICES:
        if (service == "tpcc" and os.path.exists("/opt/VDriver/.tpcc_discovery")) or \
                (service == "hxconnect" and os.path.exists("/opt/VDriver/.hxconnect_discovery")):
            port_dict = {}
            port_dict["loggerConfig"] = []
            port_dict["agentConfig"] = {}
            final_dict = add_agent_config(service, port_dict)
            discovery[SERVICE_NAME[service]] = []
            discovery[SERVICE_NAME[service]].append(final_dict)
        elif service == "esalogstore" and os.path.exists("/opt/esa_conf.json"):
            port_dict["loggerConfig"] = []
            port_dict["agentConfig"] = {}
            final_dict = add_logger_config(port_dict, service)
            discovery[SERVICE_NAME[service]] = []
            discovery[SERVICE_NAME[service]].append(final_dict)

        pid_list = get_process_id(service)
        if not pid_list:
            continue
        discovery[SERVICE_NAME[service]] = []
        for item in pid_list:
            service_pid_dict = {}
            service_pid_dict["PID"] = []

            # Add PID, cpuUsage, memUsage, status to service_discovery
            service_pid_dict["PID"] = item["process_id"]
            service_pid_dict["user"] = item["user"]
            service_pid_dict["cpuUsage"] = item["cpuUsage"]
            service_pid_dict["memUsage"] = item["memUsage"]
            service_pid_dict["status"] = item["status"]

            # Add state, threads assosciated with the service PID
            status_dict = add_status(service_pid_dict)

            # Add listening ports assosciated with the service PID
            port_dict = add_ports(status_dict, service)

            if service in POLLER_PLUGIN:
                port_dict["loggerConfig"] = []
                port_dict["agentConfig"] = {}
                final_dict = add_poller_config(service, port_dict)
            else:
                logger_dict = add_logger_config(port_dict, service)
                logger_dict["pollerConfig"] = {}
                final_dict = add_agent_config(service, logger_dict)

            discovery[SERVICE_NAME[service]].append(final_dict)
    if 'nginx' in discovery and check_nginx_plus():
        logger.info('in oonnnn')
        var = discovery.pop('nginx')[0]
        var['agentConfig'] = {'name':'nginxplus'}
        discovery['nginxplus'] = [var]

    # Hadoop plugin Start
    if parser_jcmd("org.apache.ambari.server.controller.AmbariServer"):
        for service in ["OOZIE", "HDFS", "YARN", "SPARK2"]:
            logger.info("Hadoop service is %s" %service)
            discovery[service] = []
            port_dict = {}
            port_dict["agentConfig"] = {}
            logger_dict = add_logger_config(port_dict, service)
            final_dict = add_agent_config(service, logger_dict)
            discovery[service].append(final_dict)


    # Hadoop Log Start
    try:
        hadoop_dict = dict()
        hadoop_dict["loggerConfig"] = list()
        hadoop_dict["agentConfig"] = dict()
        hadoop_dict['pollerConfig'] = dict()

        for service_name in get_hadoop_running_service_list():
            logger.info("Hadoop services are %s" %service_name)
            for service in HADOOP_SERVICE[service_name]['service-list']:
                logger.info("service detail: {}".format(service))
                port_dict = dict()
                port_dict["loggerConfig"] = list()
                port_dict["agentConfig"] = dict()
                #print(service)
                final_dict = add_logger_config(port_dict, service)
                if not final_dict["loggerConfig"]:
                    continue
                hadoop_dict["loggerConfig"].append(final_dict["loggerConfig"][0])

        if hadoop_dict["loggerConfig"]:
            discovery["hadoop-logs"] = list()
            discovery["hadoop-logs"].append(hadoop_dict)
            #print(discovery["hadoop-logs"])
    except Exception as e: print(e)
    logger.info("Discovered service %s", discovery)
    return discovery

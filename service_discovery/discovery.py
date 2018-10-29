import os
import subprocess
import psutil
import socket
import kafka
from config_handler import configurator
from common.util import *

logger = expoter_logging(COLLECTD_MGR)
service_name = {
    "elasticsearch": "ES",
    "apache": "apache",
    "mysql": "mysql",
    "mssql": "mssql",
    "postgres": "postgres",
    "nginx": "nginx",
    "tpcc": "tpcc",
    "kafka.Kafka": "kafka",
    "zookeeper": "zookeeper",
    "hxconnect" : "hxconnect",
    "cassandra": "cassandra",
    "knox": "KS",
    "esalogstore": "ESAlogstore"
}
services = [
    "elasticsearch",
    "apache",
    "mysql",
    "mssql",
    "postgres",
    "nginx",
    "tpcc",
    "kafka.Kafka",
    "zookeeper",
    "hxconnect",
    "cassandra",
    "knox",
    "esalogstore"
]
'''
Mapping for services and the plugin to be configured for them.
'''
service_plugin_mapping = {
    "elasticsearch": "elasticsearch",
    "apache": "apache",
    "mysql": "mysql",
    "mssql": "mssql",
    "knox": "oozie",
    "postgres": "postgres",
    "nginx": "nginx",
    "tpcc": "tpcc",
    "kafka.Kafka": "kafkatopic",
    "zookeeper": "zookeeperjmx",
    "hxconnect" : "hxconnect",
    "cassandra" : "cassandra",
}

poller_plugin = [
    "elasticsearch"
]

def add_pid_usage(pid, service, pid_list):
    """Add usage stats of each pids"""
    cmd = "ps auxww | grep %s | grep -v grep" % pid
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    (out, err) = p.communicate()
    for line in out.splitlines():
        if service in line:
            pid = {}
            pid["user"] = line.split()[0]
            pid["process_id"] = int(line.split()[1])
            pid["cpuUsage"] = line.split()[2]
            pid["memUsage"] = line.split()[3]
            pid["status"] = "running"
            pid_list.append(pid)


def check_jmx_enabled(pid):
    """Check if jmx enabled for java process"""
    ps_cmd = "ps -eaf | grep %s | grep -v grep" % pid
    p = subprocess.Popen(ps_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    (res, err) = p.communicate()
    if res is not "":
        res_list = res.split()
        for element in res_list:
            if element == "-Dcom.sun.management.jmxremote":
                return True
    return False

def get_process_id(service):
    '''
    :param service: name of the service
    :return: return a list of PID's assosciated with the service along with their
    status, memUsage, cpuUsage and user
    '''
    logger.info("Get process id for service %s", service)
    pids = []

    if service in ["kafka.Kafka", "zookeeper"]:
        try:
                # Common logic for jmx related process
            processID = []
            java_avail = subprocess.check_call(["java", "-version"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if not java_avail:
                jcmd = subprocess.Popen("jcmd | awk '{print $1 \" \" $2}' | grep -w \"%s\"" % service, shell=True,
                                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                res, err = jcmd.communicate()
                if res is not "":
                    j_pids = res.splitlines()
                    for j_pid in j_pids:
                        if j_pid is not "":
                            pidval = j_pid.split()
                            if check_jmx_enabled(pidval[0]):
                                processID.append(pidval[0])
    
            for procid in processID:
                add_pid_usage(procid, service, pids)
            logger.info("PIDs %s", pids)
            return pids
        except:
            logger.info("PIDs %s", pids)
            return pids

    try:
        if (service == "apache"):
            os_cmd = "lsb_release -d"
            p = subprocess.Popen(os_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            (out, err) = p.communicate()
            for line in out.splitlines():
                if ("Ubuntu" in line):
                    service = "apache2"
                    break
            # The linux flavour is not Ubuntu could be CentOS or redHat so search for httpd
            if (service == "apache"):
                service = "httpd"
    
        processID = ""
        for proc in psutil.process_iter(attrs=['pid', 'name', 'username']):
            # Java processes
            if service in ["elasticsearch", "cassandra", "knox"]:
                if proc.info.get("name") == "java" and proc.info.get("username") == service:
                    processID = proc.info.get("pid")
                    break
            # Postgres process
            elif service in ["postgres"]:
                if proc.info.get("name") == "postmaster" or proc.info.get("name") == "postgres":
                    processID = proc.info.get("pid")
                    break
            # Non java processes
            elif service in str(proc.info.get("name")):
                processID = proc.info.get("pid")
                break
    
        add_pid_usage(processID, service, pids)
        logger.info("PIDs %s", pids)
        return pids
    except:
        logger.info("PIDs %s", pids)
        return pids


def add_status(dict):
    '''
    Find the status of the PID running, sleeping.
    :param dict: dictionary return by get_process_id
    :return: add status for the PID in th dictionary.
    '''
    # Add state, threads assosciated with the service PID
    fileobj = open('/proc/%d/status' % (dict["PID"]))
    if fileobj is None:
        return
    lines = fileobj.readlines()
    for line in lines:
        if line.startswith("State:"):
            state = (line.split())[2]
            state = state.strip("()")
            dict["state"] = state
        elif line.startswith("Threads:"):
            threads = line.split()
            threads = threads[1]
            dict["threads"] = threads
    return dict


def add_ports(dict, service):
    '''
    Add listening ports for the PID
    :param dict: dictionary returned by add_status
    :param service: name of the service
    :return: add listening ports for the PID to the dictionary
    '''
    logger.debug("Add ports %s %s", dict, service)
    ports = []
    if(service == "apache"):
        apache_service = ""
        os_cmd = "lsb_release -d"
        p = subprocess.Popen(os_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        (out, err) = p.communicate()
        for line in out.splitlines():
            if("Ubuntu" in line):
                apache_service = "apache2"
                break
        if(apache_service == ""):
            apache_service = "httpd"
        cmd = "netstat -anp | grep %s" %(apache_service)
    else:
        cmd = "netstat -anp | grep %s" %(dict["PID"])
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    (out, err) = p.communicate()
    for line in out.splitlines():
        line = line.split()
        if((line[5] == 'LISTEN') and (service == "apache" or str(dict['PID']) in line[6])):
            port = (line[3].split(':')[-1])
            if port not in ports:
                ports.append(port)
    dict['ports'] = ports
    return dict


def add_logger_config(dict, service):
    dict["loggerConfig"] = []
    fluentdPlugins = configurator.get_fluentd_plugins_mapping().keys()
    for item in fluentdPlugins:
        if(item.startswith(service.split(".")[0])):
            logConfig = {}
            logConfig["name"] = item
            logConfig["config"] = {}
            logConfig["config"]["filters"] = {}
            dict["loggerConfig"].append(logConfig)
    return dict


def add_poller_config(service, dict):
    dict["pollerConfig"] = {}
    pollerConfig = {}
    pollerConfig["config"] = {}

    for key, value in service_plugin_mapping.items():
        if (key == service):
            pollerConfig["name"] = value
            break

    config = configurator.get_metrics_plugins_params(pollerConfig["name"])
    for item in config["plugins"]:
        if(item.get("config") and item.get("name") == pollerConfig["name"]):
            for item1 in item["config"]:
                pollerConfig["config"][item1["fieldName"]] = item1["defaultValue"]
    dict["pollerConfig"].update(pollerConfig)

    return dict


def add_agent_config(service, dict):
    '''
    Find the input config for the plugin fieldname:defaultvalue
    :param service: name of the service
    :param dict: poller_dict as the input
    :return:
    '''
    dict["agentConfig"] = {}
    agentConfig = {}
    agentConfig["config"] = {}
    for key, value in service_plugin_mapping.items():
        if(key == service):
            agentConfig["name"] = value
            break
    config = configurator.get_metrics_plugins_params(agentConfig["name"])
    for item in config["plugins"]:
        if(item.get("config") and item.get("name") == agentConfig["name"]):
            #Config specific to jvm plugin
            if agentConfig["name"] == "jvm":
                agentConfig["config"]["process"] = service
                break
            if agentConfig["name"] == "kafkatopic":
                agentConfig["config"]["process"] = service
                for parameter in item["config"]:
                    agentConfig["config"][parameter["fieldName"]] = parameter["defaultValue"]
                break
            for parameter in item["config"]:
                agentConfig["config"][parameter["fieldName"]] = parameter["defaultValue"]

    #In apache plugin replace the port default value with the listening ports for apache/httpd,
    #if there are multiple listening ports for the PID assosciate the first port with the PID
    if(service == "apache"):
        if(len(dict["ports"]) != 0):
            agentConfig["config"]["port"] = dict["ports"][0]
            if(agentConfig["config"]["port"] == "443"):
                agentConfig["config"]["secure"] = "true"

    dict["agentConfig"].update(agentConfig)
    return dict


def discover_services():
    '''
    Find the services which are running on the server and return it's PID list, users, CPUUsage,
    memUsage, Listening ports, input configuration for the plugin.
    :return:
    '''
    logger.info("Discover service started")
    discovery = {}
    for service in services:
        if service == "tpcc":
            '''
            If service is tpcc the check for the existance of .tpcc_discovery file
            '''
            if os.path.exists("/opt/VDriver/.tpcc_discovery"):
                port_dict = {}
                port_dict["loggerConfig"] = []
                port_dict["agentConfig"] = {}
                agent_dict = add_agent_config(service, port_dict)
                final_dict = agent_dict
                discovery[service_name[service]] = []
                discovery[service_name[service]].append(final_dict)

	if service == "hxconnect":
	    '''
	    If service is hxconnect, check for existance of .hxconnect_discovery file
	    '''
	    if os.path.exists("/opt/VDriver/.hxconnect_discovery"):
                port_dict = {}
                port_dict["loggerConfig"] = []
                port_dict["agentConfig"] = {}
                agent_dict = add_agent_config(service, port_dict)
                final_dict = agent_dict
                discovery[service_name[service]] = []
                discovery[service_name[service]].append(final_dict)

        if service == "esalogstore":
            '''
            If service is esalogstore then check for the existance of esa_conf.json file
            '''
            if os.path.exists("/opt/esa_conf.json"):
                port_dict = {}
                port_dict["loggerConfig"] = []
                port_dict["agentConfig"] = {}
                logger_dict = add_logger_config(port_dict, service)
                final_dict = logger_dict
                discovery[service_name[service]] = []
                discovery[service_name[service]].append(final_dict)

        pidList = get_process_id(service)
        if(len(pidList) != 0):
            discovery[service_name[service]] = []
            for item in pidList:
                service_pid_dict = {}
                service_pid_dict["PID"] = []
                service_pid_dict["PID"] = item["process_id"]

                #Add PID, cpuUsage, memUsage, status to service_discovery
                service_pid_dict["PID"] = item["process_id"]
                service_pid_dict["user"] = item["user"]
                service_pid_dict["cpuUsage"] = item["cpuUsage"]
                service_pid_dict["memUsage"] = item["memUsage"]
                service_pid_dict["status"] = item["status"]

                #Add state, threads assosciated with the service PID
                status_dict = add_status(service_pid_dict)

                #Add listening ports assosciated with the service PID
                port_dict = add_ports(status_dict, service)

                if service not in poller_plugin:
                    #Add logger config to the service PID
                    logger_dict = add_logger_config(port_dict, service)

                    #Add poller config to the service dict
                    logger_dict["pollerConfig"] = {}

                    #Add agent config to the service dict
                    agent_dict = add_agent_config(service, logger_dict)

                    #final_dict assosciated with the service PID
                    final_dict = agent_dict

                else:
                    port_dict["loggerConfig"] = []
                    port_dict["agentConfig"] = {}

                    # Add poller config to the service dict
                    poller_dict = add_poller_config(service, port_dict)

                    # final_dict assosciated with the service PID
                    final_dict = poller_dict

                discovery[service_name[service]].append(final_dict)

    logger.info("Discovered service %s", discovery)
    return discovery

import subprocess
from config_handler import configurator

service_name = {
    "elasticsearch": "ES",
    "apache": "apache",
    "mysql": "mysql",
    "mssql": "mssql"
}
services = [
    "elasticsearch",
    "apache",
    "mysql",
    "mssql"
]
'''
Mapping for services and the plugin to be configured for them.
'''
service_plugin_mapping = {
    "elasticsearch": "elasticsearch",
    "apache": "apache",
    "mysql": "mysql",
    "mssql": "mssql"
}

poller_plugin = [
    "elasticsearch"
]

def get_process_id(service):
    '''
    :param service: name of the service
    :return: return a list of PID's assosciated with the service along with their
    status, memUsage, cpuUsage and user
    '''
    pids = []
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
    cmd = "ps auxww | grep [" + service[:1] + "]" + service[1:]
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
            pids.append(pid)

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
    ports = []
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
        if(item.startswith(service)):
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
    for key,value in service_plugin_mapping.items():
        if(key == service):
            agentConfig["name"] = value
            break
    config = configurator.get_metrics_plugins_params(agentConfig["name"])
    for item in config["plugins"]:
        if(item.get("config") and item.get("name") == agentConfig["name"]):
            #Config specific to jvm plugin
            if(agentConfig["name"] == "jvm"):
                agentConfig["config"]["process"] = service
                break
            for item1 in item["config"]:
                agentConfig["config"][item1["fieldName"]] = item1["defaultValue"]

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
    discovery = {}
    for service in services:
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

    return discovery
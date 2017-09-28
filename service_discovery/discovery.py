import json
import subprocess
from config_handler import configurator

services = [
    "elasticsearch",
    "apache",
    "mysql",
    "mssql"
]

service_plugin_mapping = {
    "elasticsearch": "jvm",
    "apache": "apache",
    "mysql": "mysql",
    "mssql": "mssql"
}

def get_process_id(service):
    pids = []
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


def add_ports(dict):
    cmd = "netstat -anp | grep %s" %(dict["PID"])
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    (out, err) = p.communicate()
    ports = []
    for line in out.splitlines():
        line = line.split()
        if((line[5] == 'LISTEN') and (str(dict['PID']) in line[6])):
            port = (line[3].split(':')[-1])
            if port not in ports:
                ports.append(port)
    dict['ports'] = ports
    return dict


def add_logger_config(dict):
    dict["loggerConfig"] = {}
    return dict


def add_poller_config(dict):
    dict["pollerConfig"] = {}
    return dict


def add_agent_config(service, dict):
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
    dict["agentConfig"].update(agentConfig)
    return dict


def discover_services():
    discovery = {}
    for service in services:
        pidList = get_process_id(service)
        if(len(pidList) != 0):
            discovery[service] = []
            for item in pidList:
                service_pid_dict = {}

                #Add PID, cpuUsage, memUsage, status to service_discovery
                service_pid_dict["PID"] = item["process_id"]
                service_pid_dict["user"] = item["user"]
                service_pid_dict["cpuUsage"] = item["cpuUsage"]
                service_pid_dict["memUsage"] = item["memUsage"]
                service_pid_dict["status"] = item["status"]

                #Add state, threads assosciated with the service PID
                status_dict = add_status(service_pid_dict)

                #Add listening ports assosciated with the service PID
                port_dict = add_ports(status_dict)

                #Add logger config to the service PID
                logger_dict = add_logger_config(port_dict)

                #Add poller config to the service dict
                poller_dict = add_poller_config(logger_dict)

                #Add agent config to the service dict
                agent_dict = add_agent_config(service, poller_dict)

                #final_dict assosciated with the service PID
                final_dict = agent_dict
                discovery[service].append(final_dict)

    return discovery
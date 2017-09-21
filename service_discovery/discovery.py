import json
import subprocess

services = [
    "elasticsearch",
    "apache",
    "mysql",
    "mssql"
]

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
                #final_dict assosciated with the service PID
                final_dict = port_dict
                discovery[service].append(final_dict)
        #The service is not running on the server
        else:
            discovery[service] = {}
            discovery[service]["status"] = "not running"

    return discovery



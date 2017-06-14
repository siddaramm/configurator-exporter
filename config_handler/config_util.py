from stat_exporter.collectd_exporter import *


def push_collectd_configaration(success_config):
    error_msg = ''
    try:
        if success_config:
            conf_files = []
            for cfg in success_config:
                file_writer(cfg[0], cfg[1] + "\n")
                conf_files.append(cfg[0])

            insert_collectd_conf('python', conf_files)
            return True, error_msg

    except Exception as e:
        error_msg += "push configaration: "
        error_msg += str(e)
        logger.error(error_msg)
    return False, error_msg


def delete_collectd_config():
    error_msg = ''
    try:
        command = "ls " + CollectdPluginConfDir
        ret = run_command(command.split())
        for value in ret:
            filepath = CollectdPluginConfDir + os.path.sep + value
            file_delete(filepath)

        delete_collectd_conf()
        file_writer(CollectdData, json.dumps({}))
        delete_all_stats()
        return True, error_msg
    except Exception as e:
        error_msg += "delete config: "
        error_msg += str(e)
        logger.error(error_msg)
    return False, error_msg


def get_collectd_plugins_mapping():
    dirname, filename = os.path.split(os.path.abspath(__file__))
    file_name = os.path.join(dirname,CollectdPluginMappingFilePath)
    return read_yaml_file(file_name)


def get_supported_targets_mapping():
    dirname, filename = os.path.split(os.path.abspath(__file__))
    file_name = os.path.join(dirname, TargetMappingFilePath)
    return read_yaml_file(file_name)


def get_collectd_plugin_names(mapped_plugin=None):
    result = []
    mapping_list = get_collectd_plugins_mapping()
    if mapping_list:
        # mapping_list = json.loads(mapping_list)
        # plugin_mapping = mapping_list.get(PLUGINS, {})
        # print plugin_mapping
        if mapped_plugin is None:
            for i, v in mapping_list.items():
                # print i, v
                result.append(i)
                for p in v:
                    # print p
                    result.append(p["name"])
        else:
            for p in mapping_list.get(mapped_plugin, []):
                result.append(p["name"])
    return list(set(result))


def get_dest_filename(plugin):
    return os.path.join(CollectdPluginConfDir, plugin + EXTSN)


def change_fluentd_status(oper):
    service = "td-agent"
    status = get_service_status(service)
    if status == -1:
        logger.warning("td-agent service not found, fluentd not installed")
    elif oper == "start":
        start_service(service)
    elif oper == "stop":
        stop_service(service)
    elif oper == "restart":
        restart_service(service)
    else:
        logger.error("Undefined operation %s, start|stop|restart", oper)


def get_fluentd_status():
    service = "td-agent"
    status = get_service_status(service)
    if status == -1:
        return "NOT INSTALLED"
    if status == 1:
        return "RUNNING"
    elif status == 0:
        return "STOPPED"
    else:
        return "NOT RUNNING"


def get_fluentd_version():
    """
    Get collectd version
    :return:
    """
    # try:
    service = "td-agent"
    status = get_service_status(service)
    if status == -1:
        return "NOT INSTALLED"
    command = "td-agent --version | awk {'print $2'}"
    out, err = run_shell_command(command)
    if err:
        logger.warning("Failed to get output")
        return "UNKNOWN"
    if out.splitlines():
        for line in out.splitlines():
            line = str(line)
            if line and line[0].isdigit():
                return line
    return "UNKNOWN"

def start_collectd():
    service = "collectd"
    status = get_service_status(service)
    if status == -1:
        pid = get_process_id(service)
        if pid != -1:
            kill_process(pid)
        command = COLLECTDBIN + " -C " + CollectdConfDir + "/collectd.conf"
        out, err = run_shell_command(command)
    elif status == 0:
        out, err = start_service(service)
    else:
        out, err = restart_service(service)

    pid = get_process_id(service)
    if pid == -1:
        command = COLLECTDBIN + " -C " + CollectdConfDir + "/collectd.conf"
        out, err = run_shell_command(command)
    if err:
        logger.warning("Failed to start collectd" + str(err))
        

def stop_collectd():
    # command = "service collectd stop".split()
    # for line in run_command(command):
    #     print (line)
    service = "collectd"
    status = get_service_status(service)
    if status == 1:
        stop_service(service)
    pid = get_process_id(service)
    if pid != -1:
        kill_process(pid)


def get_collectd_status():
    service = "collectd"
    status = get_service_status(service)
    if status == -1:
        pid = get_process_id(service)
        if pid != -1:
            status = 1
    if status == 1:
        return "RUNNING"
    elif status == 0:
        return "STOPPED"
    else:
        return "NOT RUNNING"


def get_collectd_version():
    """
    Get collectd version
    :return:
    """
    # try:
    command = COLLECTDBIN + " -help | grep ^collectd | awk {'print $2'}"
    out, err = run_shell_command(command)
    if err:
        logger.warning("Failed to get output")
        return "UNKNOWN"
    if out.splitlines():
        for line in out.splitlines():
            line = str(line)
            if line and line[0].isdigit():
                return line[0:5]
    return "UNKNOWN"


def insert_collectd_conf(loadplugin, conf_file=None):
    if conf_file is None:
        conf_file = []
    collectd_conf = CollectdConfDir + os.path.sep + 'collectd.conf'
    data = None
    try:
        f = open(collectd_conf, "r")
        data = f.readlines()
        f.close()
    except:
        logger.error("Error in %s File Reading ", collectd_conf)
    loadplugin = "LoadPlugin " + loadplugin + "\n"
    if data:
        new_data = [loadplugin]
        for f in conf_file:
            addconf = "Include \"" + f + "\"\n"
            new_data.append(addconf)
        for line in data:
            if line.startswith(loadplugin) or line.startswith("Include "):
                continue
            else:
                new_data.append(line)

        # Write configuration to file
        f = open(collectd_conf, 'w')
        f.write(''.join(new_data))
        f.close()


def delete_collectd_conf():
    collectd_conf = CollectdConfDir + os.path.sep + 'collectd.conf'
    data = None
    try:
        f = open(collectd_conf, "r")
        data = f.readlines()
        f.close()
    except:
        logger.error("Error in %s File Reading ", collectd_conf)

    # addconf = "Include \"" + filepath + "\"\n"

    if data:
        new_data = []
        for line in data:
            if line.startswith("Include "):
                continue
            else:
                new_data.append(line)

        # Write configuration to file
        f = open(collectd_conf, 'w')
        f.write(''.join(new_data))
        f.close()


def get_fluentd_plugins_mapping():
    dirname, filename = os.path.split(os.path.abspath(__file__))
    file_name = os.path.join(dirname, FluentdPluginMappingFilePath)
    return read_yaml_file(file_name)
    # return read_yaml_file(FluentdPluginMappingFilePath)


def delete_fluentd_config():
    error_msg = ''
    fluentd_conf_dir = FluentdPluginConfDir + os.path.sep

    try:
        exsting_data = file_reader(FluentdData)
        if exsting_data:
            exsting_data = json.loads(exsting_data)
        else:
            exsting_data = {}

        if LOGGING in exsting_data:
            for x_plugin in exsting_data[LOGGING]:

                if NAME in x_plugin:
                    fluentd_conf_filepath = fluentd_conf_dir + x_plugin[NAME]
                    command = "rm -rf " + fluentd_conf_filepath
                    out, err = run_shell_command(command)
                    if err:
                        msg = "Failed to Delete Fluentd conf file " + fluentd_conf_filepath
                        logger.warning(msg)

            fluentd_conf_filepath = fluentd_conf_dir + "td-agent.conf"
            command = "rm -rf " + fluentd_conf_filepath
            out, err = run_shell_command(command)
            if err:
                msg = "Failed to Delete Fluentd conf file " + fluentd_conf_filepath
                logger.warning(msg)
        return True, error_msg

    except Exception as e:
        error_msg += "delete fluentd config: "
        error_msg += str(e)
        logger.error(error_msg)
    return False, error_msg

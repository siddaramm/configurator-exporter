from config_handler import collectd_manager
from config_handler import fluentd_manager
from config_util import *


def set_collectd_config(metrics):
    """
    API endpoint for configuring metrics config
    :param metrics:
    :return:
    """
    create_plugin_env()
    collector_obj = collectd_manager.CollectdManager(metrics)
    success, return_dict = collector_obj.set_config()
    if ERROR not in  return_dict:
        return_dict[COLLECTD_STATUS] = get_collectd_process()
    return return_dict


def get_collectd_config():
    """
    API endpoint for get config from collectd
    :return:
    """
    exsting_data = file_reader(CollectdData)
    if exsting_data:
        exsting_data = json.loads(exsting_data)
        exsting_data[COLLECTD_STATUS] = get_collectd_process()
    else:
        exsting_data = {}
    return exsting_data


def enabled_collectd(data):
    """
    API endpoint for enable or disable collectd process
    :param data:
    :return:
    """
    if data[ENABLED]:
        start_collectd()
        result = get_collectd_status()

    else:
        stop_collectd()
        result = get_collectd_status()

    return result


def get_collectd_process():
    """
    API endpoint for get collectd status
    :return:
    """
    return {COLLECTD_SERVICE: get_collectd_status(), VERSION: get_collectd_version()}


def enabled_fluentd(data):
    """
    API endpoint for enable or disable fluentd process
    :param data:
    :return:
    """
    if data[ENABLED]:
        change_fluentd_status("start")
        result = get_fluentd_status()

    else:
        change_fluentd_status("stop")
        result = get_fluentd_status()

    return result


def get_fluentd_process():
    """
    API endpoint for get fluentd status
    :return:
    """
    return {FLUENTD_SERVICE: get_fluentd_status(), VERSION: get_fluentd_version()}


def set_fluentd_config(logging):
    """
    API endpoint for configuring logging config
    :param logging:
    :return:
    """
    return_dict = None
    obj = fluentd_manager.FluentdPluginManager(logging)
    return_dict = obj.set_config()
    if ERROR not in return_dict:
        return_dict[FLUENTD_STATUS] = get_fluentd_process()
    # collector_obj = collectd_manager.CollectdManager(logging)
    # success, return_dict = collector_obj.set_config()
    return return_dict


def get_fluentd_config():
    exsting_data = file_reader(FluentdData)
    if exsting_data:
        exsting_data = json.loads(exsting_data)
        exsting_data[FLUENTD_STATUS]= get_fluentd_process()
    else:
        exsting_data = {}
    return exsting_data


def get_supported_metrics_plugins():
    mapping_list = get_collectd_plugins_mapping()
    result = []
    for key in mapping_list:
        result.append(key)
    return result


def get_supported_logging_plugins():
    mapping_list = get_fluentd_plugins_mapping()
    result = []
    for key in mapping_list:
        result.append(key)
    return result

def get_supported_targets():
    mapping_list = get_supported_targets_mapping()
    result = []
    for key in mapping_list:
        result.append(key)
    return result

def get_targets_params(targets=None):
    if targets is None:
        targets = []
    mapping_list = get_supported_targets_mapping()
    result = []
    for key, value in mapping_list.items():
        if targets and key not in targets:
            continue
        result.append(value)
    return result

def get_metrics_plugins_params(plugins=None):
    """
    Get the names of plugin and configurable parameters
    :param plugins:
    :return:
    """
    if plugins is None:
        plugins = []
    result = {PLUGINS: []}
    mapping_list = get_collectd_plugins_mapping()
    for name, value in mapping_list.items():
        if plugins and name not in plugins:
            continue
        data = {NAME: name}
        config_data = {}
        for v in value:
            config_data.update(v.get(CONFIG_DATA, {}))
        if config_data:
            data[CONFIG_DATA] = config_data
        result[PLUGINS].append(data)
    return result

def get_logging_plugins_params(plugins=None):
    if plugins is None:
        plugins = []
    result = {PLUGINS: []}
    for i in get_supported_logging_plugins():
        if plugins and i not in plugins:
            continue
        result[PLUGINS].append({NAME: i, "filter": []})

    return result

def map_local_targets(targets,data):
    n_targets = list()
    if TARGETS in data:
        for l_targets in data[TARGETS]:
            for g_targets in targets:
                if l_targets == g_targets.get(NAME):
                    n_targets.append(g_targets)
    if not n_targets:
        logger.info("local targets is not matching with Global Targets")
    data[TARGETS] = n_targets
    return data






import logging

import pytest
import sdk_cmd
import sdk_hosts
import sdk_install
import sdk_marathon
import sdk_metrics
import sdk_networks
import sdk_plan
import sdk_tasks
import sdk_upgrade
import sdk_utils
from tests import config

log = logging.getLogger(__name__)

foldered_name = sdk_utils.get_foldered_name(config.SERVICE_NAME)
current_expected_task_count = config.DEFAULT_TASK_COUNT


@pytest.fixture(scope="module", autouse=True)
def configure_package(configure_security):
    try:
        log.info("Ensure elasticsearch and kibana are uninstalled...")
        sdk_install.uninstall(config.KIBANA_PACKAGE_NAME, config.KIBANA_PACKAGE_NAME)
        sdk_install.uninstall(config.PACKAGE_NAME, foldered_name)

        # sdk_upgrade.test_upgrade(
        #     config.PACKAGE_NAME,
        #     foldered_name,
        #     current_expected_task_count,
        #     additional_options={"service": {"name": foldered_name}},
        # )
        sdk_install.install(config.PACKAGE_NAME, foldered_name, current_expected_task_count)

        yield  # let the test session execute
    finally:
        log.info("Clean up elasticsearch and kibana...")
        sdk_install.uninstall(config.KIBANA_PACKAGE_NAME, config.KIBANA_PACKAGE_NAME)
        sdk_install.uninstall(config.PACKAGE_NAME, foldered_name)


@pytest.fixture(autouse=True)
def pre_test_setup():
    sdk_tasks.check_running(foldered_name, current_expected_task_count)
    config.wait_for_expected_nodes_to_exist(
        service_name=foldered_name, task_count=current_expected_task_count
    )


@pytest.fixture
def default_populated_index():
    config.delete_index(config.DEFAULT_INDEX_NAME, service_name=foldered_name)
    config.create_index(
        config.DEFAULT_INDEX_NAME, config.DEFAULT_SETTINGS_MAPPINGS, service_name=foldered_name
    )
    config.create_document(
        config.DEFAULT_INDEX_NAME,
        config.DEFAULT_INDEX_TYPE,
        1,
        {"name": "Loren", "role": "developer"},
        service_name=foldered_name,
    )


@pytest.mark.recovery
@pytest.mark.sanity
def test_pod_replace_then_immediate_config_update():
    sdk_cmd.svc_cli(config.PACKAGE_NAME, foldered_name, "pod replace data-0")

    sdk_upgrade.update_or_upgrade_or_downgrade(
        config.PACKAGE_NAME,
        foldered_name,
        None,
        {
            "service": {"upgrade_strategy": "parallel"},
            "elasticsearch": {"plugins": "analysis-phonetic"},
        },
        current_expected_task_count,
    )

    # Ensure all nodes, especially data-0, get launched with the updated config.
    config.check_elasticsearch_plugin_installed(plugin_name, service_name=foldered_name)
    sdk_plan.wait_for_completed_deployment(foldered_name)
    sdk_plan.wait_for_completed_recovery(foldered_name)


@pytest.mark.sanity
def test_endpoints():
    # Check that we can reach the scheduler via admin router, and that returned endpoints are
    # sanitized.
    for endpoint in config.ENDPOINT_TYPES:
        endpoints = sdk_networks.get_endpoint(config.PACKAGE_NAME, foldered_name, endpoint)
        host = endpoint.split("-")[0]  # 'coordinator-http' => 'coordinator'
        assert endpoints["dns"][0].startswith(
            sdk_hosts.autoip_host(foldered_name, host + "-0-node")
        )
        assert endpoints["vip"].startswith(sdk_hosts.vip_host(foldered_name, host))

    sdk_plan.wait_for_completed_deployment(foldered_name)
    sdk_plan.wait_for_completed_recovery(foldered_name)


@pytest.mark.sanity
def test_indexing(default_populated_index):
    indices_stats = config.get_elasticsearch_indices_stats(
        config.DEFAULT_INDEX_NAME, service_name=foldered_name
    )
    assert indices_stats["_all"]["primaries"]["docs"]["count"] == 1
    doc = config.get_document(
        config.DEFAULT_INDEX_NAME, config.DEFAULT_INDEX_TYPE, 1, service_name=foldered_name
    )
    assert doc["_source"]["name"] == "Loren"

    sdk_plan.wait_for_completed_deployment(foldered_name)
    sdk_plan.wait_for_completed_recovery(foldered_name)


@pytest.mark.sanity
@pytest.mark.dcos_min_version("1.9")
def test_metrics():
    expected_metrics = [
        "node.data-0-node.fs.total.total_in_bytes",
        "node.data-0-node.jvm.mem.pools.old.peak_used_in_bytes",
        "node.data-0-node.jvm.threads.count",
    ]

    def expected_metrics_exist(emitted_metrics):
        # Elastic metrics are also dynamic and based on the service name# For eg:
        # elasticsearch.test__integration__elastic.node.data-0-node.thread_pool.listener.completed
        # To prevent this from breaking we drop the service name from the metric name
        # => data-0-node.thread_pool.listener.completed
        metric_names = [".".join(metric_name.split(".")[2:]) for metric_name in emitted_metrics]
        return sdk_metrics.check_metrics_presence(metric_names, expected_metrics)

    sdk_metrics.wait_for_service_metrics(
        config.PACKAGE_NAME,
        foldered_name,
        "data-0",
        "data-0-node",
        config.DEFAULT_TIMEOUT,
        expected_metrics_exist,
    )

    sdk_plan.wait_for_completed_deployment(foldered_name)
    sdk_plan.wait_for_completed_recovery(foldered_name)


@pytest.mark.sanity
def test_custom_yaml_base64():
    # Apply this custom YAML block as a base64-encoded string:

    # cluster:
    #   routing:
    #     allocation:
    #       node_initial_primaries_recoveries: 3

    # The default value is 4. We're just testing to make sure the YAML formatting survived intact and the setting
    # got updated in the config.
    base64_elasticsearch_yml = "Y2x1c3RlcjoNCiAgcm91dGluZzoNCiAgICBhbGxvY2F0aW9uOg0KICAgICAgbm9kZV9pbml0aWFsX3ByaW1hcmllc19yZWNvdmVyaWVzOiAz"

    sdk_upgrade.update_or_upgrade_or_downgrade(
        config.PACKAGE_NAME,
        foldered_name,
        None,
        {"elasticsearch": {"custom_elasticsearch_yml": base64_elasticsearch_yml}},
        current_expected_task_count,
    )

    config.check_custom_elasticsearch_cluster_setting(service_name=foldered_name)

    sdk_plan.wait_for_completed_deployment(foldered_name)
    sdk_plan.wait_for_completed_recovery(foldered_name)


# TODO(mpereira): it is safe to remove this test after the 6.x release.
@pytest.mark.sanity
@pytest.mark.timeout(60 * 60)
def test_xpack_upgrade_matrix():
    log.info("X-Pack from 'enabled' to 'enabled'")
    sdk_upgrade.test_upgrade(
        config.PACKAGE_NAME,
        foldered_name,
        config.DEFAULT_TASK_COUNT,
        additional_options={"elasticsearch": {"xpack_enabled": True}},
        test_version_additional_options={
            "service": {"update_strategy": "parallel"},
            "elasticsearch": {"xpack_enabled": True},
        },
    )

    log.info("X-Pack from 'enabled' to 'disabled'")
    sdk_upgrade.test_upgrade(
        config.PACKAGE_NAME,
        foldered_name,
        config.DEFAULT_TASK_COUNT,
        additional_options={"elasticsearch": {"xpack_enabled": True}},
        test_version_additional_options={
            "service": {"update_strategy": "parallel"},
            "elasticsearch": {"xpack_enabled": False},
        },
    )

    log.info("X-Pack from 'disabled' to 'disabled'")
    sdk_upgrade.test_upgrade(
        config.PACKAGE_NAME,
        foldered_name,
        config.DEFAULT_TASK_COUNT,
        additional_options={"elasticsearch": {"xpack_enabled": False}},
        test_version_additional_options={
            "service": {"update_strategy": "parallel"},
            "elasticsearch": {"xpack_enabled": False},
        },
    )

    log.info("X-Pack from 'disabled' to 'enabled'")
    sdk_upgrade.test_upgrade(
        config.PACKAGE_NAME,
        foldered_name,
        config.DEFAULT_TASK_COUNT,
        additional_options={"elasticsearch": {"xpack_enabled": False}},
        test_version_additional_options={
            "service": {"update_strategy": "parallel"},
            "elasticsearch": {"xpack_enabled": True},
        },
    )


@pytest.mark.sanity
@pytest.mark.timeout(60 * 60)
def test_xpack_toggle_with_kibana(default_populated_index):
    log.info("\n***** Verify commercial APIs disabled by default in Elasticsearch")
    config.verify_commercial_api_status(is_expected_to_be_enabled=False, service_name=foldered_name)

    log.info("\n***** Test kibana")
    elasticsearch_url = "http://" + sdk_hosts.vip_host(foldered_name, "coordinator", 9200)
    sdk_install.install(
        config.KIBANA_PACKAGE_NAME,
        config.KIBANA_PACKAGE_NAME,
        0,
        {"kibana": {"elasticsearch_url": elasticsearch_url}},
        timeout_seconds=config.KIBANA_DEFAULT_TIMEOUT,
        wait_for_deployment=False,
        insert_strict_options=False,
    )
    config.check_kibana_adminrouter_integration("service/{}/".format(config.KIBANA_PACKAGE_NAME))
    log.info("Uninstall kibana with X-Pack disabled")
    sdk_install.uninstall(config.KIBANA_PACKAGE_NAME, config.KIBANA_PACKAGE_NAME)

    log.info(
        "\n***** Set/verify X-Pack enabled in elasticsearch. Requires parallel upgrade strategy for full restart."
    )
    sdk_upgrade.update_or_upgrade_or_downgrade(
        config.PACKAGE_NAME,
        foldered_name,
        None,
        {"service": {"upgrade_strategy": "parallel"}, "elasticsearch": {"xpack_enabled": True}},
        current_expected_task_count,
    )
    config.verify_commercial_api_status(False, service_name=foldered_name)

    # Verify basic license is enabled by default,
    config.verify_xpack_license("basic", service_name=foldered_name)

    # Start trial license.
    config.start_trial_license(service_name=foldered_name)

    # Verify trial license is working.
    config.verify_xpack_license("trial", service_name=foldered_name)
    config.verify_commercial_api_status(True, service_name=foldered_name)

    log.info(
        "\n***** Write some data while enabled, disable X-Pack, and verify we can still read what we wrote."
    )
    config.create_document(
        config.DEFAULT_INDEX_NAME,
        config.DEFAULT_INDEX_TYPE,
        2,
        {"name": "X-Pack", "role": "commercial plugin"},
        service_name=foldered_name,
    )

    log.info("\n***** Test kibana with X-Pack enabled...")
    sdk_install.install(
        config.KIBANA_PACKAGE_NAME,
        config.KIBANA_PACKAGE_NAME,
        0,
        {"kibana": {"elasticsearch_url": elasticsearch_url, "xpack_enabled": True}},
        timeout_seconds=config.KIBANA_DEFAULT_TIMEOUT,
        wait_for_deployment=False,
        insert_strict_options=False,
    )
    config.check_kibana_adminrouter_integration("service/{}/".format(config.KIBANA_PACKAGE_NAME))
    log.info("\n***** Uninstall kibana with X-Pack enabled")
    sdk_install.uninstall(config.KIBANA_PACKAGE_NAME, config.KIBANA_PACKAGE_NAME)

    log.info("\n***** Disable X-Pack in elasticsearch.")
    sdk_upgrade.update_or_upgrade_or_downgrade(
        config.PACKAGE_NAME,
        foldered_name,
        None,
        {"service": {"upgrade_strategy": "serial"}, "elasticsearch": {"xpack_enabled": False}},
        current_expected_task_count,
    )
    log.info("\n***** Verify we can still read what we wrote when X-Pack was enabled.")
    doc = config.get_document(
        config.DEFAULT_INDEX_NAME, config.DEFAULT_INDEX_TYPE, 2, service_name=foldered_name
    )
    assert doc["_source"]["name"] == "X-Pack"

    sdk_plan.wait_for_completed_deployment(foldered_name)
    sdk_plan.wait_for_completed_recovery(foldered_name)


@pytest.mark.recovery
@pytest.mark.sanity
def test_losing_and_regaining_index_health(default_populated_index):
    config.check_elasticsearch_index_health(
        config.DEFAULT_INDEX_NAME, "green", service_name=foldered_name
    )
    sdk_cmd.kill_task_with_pattern(
        "data__.*Elasticsearch",
        "nobody",
        agent_host=sdk_tasks.get_service_tasks(foldered_name, "data-0-node")[0].host,
    )
    config.check_elasticsearch_index_health(
        config.DEFAULT_INDEX_NAME, "yellow", service_name=foldered_name
    )
    config.check_elasticsearch_index_health(
        config.DEFAULT_INDEX_NAME, "green", service_name=foldered_name
    )

    sdk_plan.wait_for_completed_deployment(foldered_name)
    sdk_plan.wait_for_completed_recovery(foldered_name)


@pytest.mark.recovery
@pytest.mark.sanity
def test_master_reelection():
    initial_master = config.get_elasticsearch_master(service_name=foldered_name)
    sdk_cmd.kill_task_with_pattern(
        "master__.*Elasticsearch",
        "nobody",
        agent_host=sdk_tasks.get_service_tasks(foldered_name, initial_master)[0].host,
    )
    sdk_plan.wait_for_in_progress_recovery(foldered_name)
    sdk_plan.wait_for_completed_recovery(foldered_name)
    config.wait_for_expected_nodes_to_exist(service_name=foldered_name)
    new_master = config.get_elasticsearch_master(service_name=foldered_name)
    assert new_master.startswith("master") and new_master != initial_master

    sdk_plan.wait_for_completed_deployment(foldered_name)
    sdk_plan.wait_for_completed_recovery(foldered_name)


@pytest.mark.recovery
@pytest.mark.sanity
def test_master_node_replace():
    # Ideally, the pod will get placed on a different agent. This test will verify that the remaining two masters
    # find the replaced master at its new IP address. This requires a reasonably low TTL for Java DNS lookups.
    sdk_cmd.svc_cli(config.PACKAGE_NAME, foldered_name, "pod replace master-0")
    sdk_plan.wait_for_in_progress_recovery(foldered_name)
    sdk_plan.wait_for_completed_recovery(foldered_name)


@pytest.mark.recovery
@pytest.mark.sanity
def test_data_node_replace():
    sdk_cmd.svc_cli(config.PACKAGE_NAME, foldered_name, "pod replace data-0")
    sdk_plan.wait_for_in_progress_recovery(foldered_name)
    sdk_plan.wait_for_completed_recovery(foldered_name)


@pytest.mark.recovery
@pytest.mark.sanity
def test_coordinator_node_replace():
    sdk_cmd.svc_cli(config.PACKAGE_NAME, foldered_name, "pod replace coordinator-0")
    sdk_plan.wait_for_in_progress_recovery(foldered_name)
    sdk_plan.wait_for_completed_recovery(foldered_name)


@pytest.mark.recovery
@pytest.mark.sanity
@pytest.mark.timeout(60 * 60)
def test_plugin_install_and_uninstall(default_populated_index):
    plugins = "analysis-icu"

    sdk_upgrade.update_or_upgrade_or_downgrade(
        config.PACKAGE_NAME,
        foldered_name,
        None,
        {"elasticsearch": {"plugins": plugins}},
        current_expected_task_count,
    )
    config.check_elasticsearch_plugin_installed(plugins, service_name=foldered_name)

    sdk_upgrade.update_or_upgrade_or_downgrade(
        config.PACKAGE_NAME,
        foldered_name,
        None,
        {"elasticsearch": {"plugins": ""}},
        current_expected_task_count,
    )
    config.check_elasticsearch_plugin_uninstalled(plugins, service_name=foldered_name)

    sdk_plan.wait_for_completed_deployment(foldered_name)
    sdk_plan.wait_for_completed_recovery(foldered_name)


@pytest.mark.recovery
@pytest.mark.sanity
def test_bump_node_counts():
    # bump ingest and coordinator, but NOT data, which is bumped in the following test.
    # we want to avoid adding two data nodes because the cluster sometimes won't have enough room for it
    marathon_config = sdk_marathon.get_config(foldered_name)
    ingest_nodes_count = int(marathon_config["env"]["INGEST_NODE_COUNT"])
    coordinator_nodes_count = int(marathon_config["env"]["COORDINATOR_NODE_COUNT"])

    global current_expected_task_count
    current_expected_task_count += 2

    sdk_upgrade.update_or_upgrade_or_downgrade(
        config.PACKAGE_NAME,
        foldered_name,
        None,
        {"ingest_nodes": {"count": str(ingest_nodes_count + 1)}},
        {"coordinator_nodes": {"count": str(coordinator_nodes_count + 1)}},
        current_expected_task_count,
    )

    sdk_tasks.check_running(foldered_name, current_expected_task_count)
    sdk_plan.wait_for_completed_deployment(foldered_name)
    sdk_plan.wait_for_completed_recovery(foldered_name)


@pytest.mark.recovery
@pytest.mark.sanity
def test_adding_data_node_only_restarts_masters():
    initial_master_task_ids = sdk_tasks.get_task_ids(foldered_name, "master")
    initial_data_task_ids = sdk_tasks.get_task_ids(foldered_name, "data")
    initial_coordinator_task_ids = sdk_tasks.get_task_ids(foldered_name, "coordinator")

    data_nodes_count = int(sdk_marathon.get_config(foldered_name)["env"]["DATA_NODE_COUNT"])

    global current_expected_task_count
    current_expected_task_count += 1

    sdk_upgrade.update_or_upgrade_or_downgrade(
        config.PACKAGE_NAME,
        foldered_name,
        None,
        {"data_nodes": {"count": str(data_nodes_count + 1)}},
        current_expected_task_count,
    )

    sdk_tasks.check_running(foldered_name, current_expected_task_count)
    sdk_tasks.check_tasks_updated(foldered_name, "master", initial_master_task_ids)
    sdk_tasks.check_tasks_not_updated(foldered_name, "data", initial_data_task_ids)
    sdk_tasks.check_tasks_not_updated(foldered_name, "coordinator", initial_coordinator_task_ids)
    sdk_plan.wait_for_completed_deployment(foldered_name)
    sdk_plan.wait_for_completed_recovery(foldered_name)

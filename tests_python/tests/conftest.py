"""Hooks and fixtures.

A fixture defines code to be run before and after a (sequence of) test,
E.g. start and stop a server. The fixture is simply specified as a parameter
in the test function, and the yielded values is then accessible with this
parameter.
"""
import tempfile
import subprocess
import shutil
import os
from typing import Optional
import pytest
from pytest_regtest import register_converter_pre, deregister_converter_pre, \
    _std_conversion
from launchers.sandbox import Sandbox, SandboxMultiBranch
from tools import constants, paths, utils
from tools.client_regression import ClientRegression


@pytest.fixture(scope="session", autouse=True)
def sanity_check(request):
    """Sanity checks before running the tests."""
    log_dir = request.config.getoption("--log-dir")
    if not (log_dir is None or os.path.isdir(log_dir)):
        print(f"{log_dir} doesn't exist")
        pytest.exit(1)


@pytest.fixture(scope="session")
def log_dir(request):
    """Retrieve user-provided logging directory on the command line."""
    yield request.config.getoption("--log-dir")


@pytest.fixture(scope="session")
def singleprocess(request):
    """Retrieve user-provided single process mode on the command line."""
    yield request.config.getoption("--singleprocess")


@pytest.fixture(scope="class")
def session():
    """Dictionary to store data between tests."""
    yield {}


def pytest_runtest_makereport(item, call):
    # hook for incremental test
    # from https://docs.pytest.org/en/latest/example/simple.html
    if "incremental" in item.keywords:
        if call.excinfo is not None:
            parent = item.parent
            # TODO can we do without this hack?
            parent._previousfailed = item  # pylint: disable=protected-access


def pytest_runtest_setup(item):
    if "incremental" in item.keywords:
        previousfailed = getattr(item.parent, "_previousfailed", None)
        if previousfailed is not None:
            pytest.xfail("previous test failed (%s)" % previousfailed.name)


def pytest_addoption(parser):
    parser.addoption(
        "--log-dir", action="store", help="specify log directory"
    )
    parser.addoption(
        "--singleprocess", action='store_true', help="the node validates \
        blocks using only one process, useful for debugging")


DEAD_DAEMONS_WARN = '''
It seems some daemons terminated unexpectedly, or didn't launch properly.
You can investigate daemon logs by running this test using the
`--log-dir=LOG_DIR` option.'''


@pytest.fixture(scope="class")
def sandbox(log_dir):
    """Sandboxed network of nodes.

    Nodes, bakers and endorsers are added/removed dynamically."""
    # log_dir is None if not provided on command-line
    # singleprocess is false if not provided on command-line
    with Sandbox(paths.TEZOS_HOME,
                 constants.IDENTITIES,
                 log_dir=log_dir,
                 singleprocess=singleprocess) as sandbox:
        yield sandbox
        assert sandbox.are_daemons_alive(), DEAD_DAEMONS_WARN


@pytest.fixture(scope="class")
def client(sandbox):
    """One node with protocol alpha."""
    sandbox.add_node(0, params=constants.NODE_PARAMS)
    client = sandbox.client(0)
    utils.activate_alpha(client)
    yield client


@pytest.fixture(scope="class")
def client_regtest_bis(sandbox):
    """One node with protocol alpha, regression test enabled."""
    def reg_client_factory(client_path: str,
                           admin_client_path: str,
                           host: str = '127.0.0.1',
                           base_dir: Optional[str] = None,
                           rpc_port: int = 8732,
                           use_tls: bool = False,
                           disable_disclaimer: bool = True):
        client = ClientRegression(client_path,
                                  admin_client_path,
                                  host,
                                  base_dir,
                                  rpc_port,
                                  use_tls,
                                  disable_disclaimer)
        return client

    sandbox.add_node(1, client_factory=reg_client_factory,
                     params=constants.NODE_PARAMS)
    client = sandbox.client(1)
    utils.activate_alpha(client)
    yield client


@pytest.fixture(scope="function")
def client_regtest(client_regtest_bis, regtest):
    """The client for one node with protocol alpha, with a function level
regression test fixture."""
    deregister_converter_pre(_std_conversion)
    client_regtest_bis.set_regtest(regtest)
    register_converter_pre(utils.client_always_output_converter)
    yield client_regtest_bis
    deregister_converter_pre(utils.client_always_output_converter)


@pytest.fixture(scope="function")
def client_regtest_scrubbed(client_regtest):
    """One node with protocol alpha, regression test and scrubbing enabled."""
    register_converter_pre(utils.client_output_converter)
    yield client_regtest
    deregister_converter_pre(utils.client_output_converter)


@pytest.fixture(scope="class")
def clients(sandbox, request):
    """N node with protocol alpha. Parameterized by the number of nodes.

    Number of nodes is specified as a class annotation.
    @pytest.mark.parametrize('clients', [N], indirect=True)
    """
    assert request.param is not None
    num_nodes = request.param
    for i in range(num_nodes):
        # Large number may increases peers connection time
        sandbox.add_node(i, params=constants.NODE_PARAMS)
    utils.activate_alpha(sandbox.client(0))
    clients = sandbox.all_clients()
    for client in clients:
        proto = constants.ALPHA
        assert utils.check_protocol(client, proto)
    yield clients


@pytest.fixture(scope="class")
def sandbox_multibranch(log_dir, request):
    """Multi-branch sandbox fixture. Parameterized by map of branches.

    This fixture is identical to `sandbox` except that each node_id is
    mapped to a pair (git revision, protocol version). For instance,
    suppose a mapping:

      MAP = { 0: ('zeronet', 'alpha'), 1:('mainnet', '003-PsddFKi3'),
              2: ('alphanet', '003-PsddFKi3' }

    If we annotate the class test as follows.
    @pytest.mark.parametrize('sandbox_multibranch', [MAP], indirect=True)

    The executables (node, baker, endorser)
    - for node_id 0 will be looked up in `TEZOS_BINARY/zeronet`,
    - for node_id 1 will be looked up in `TEZOS_BINARY/mainnet` and so on...

    baker and endorser will use the specified protocol version, according
    to the tezos executables naming conventions.
    """
    if paths.TEZOS_BINARIES is None:
        pytest.skip()
    branch_map = request.param
    assert branch_map is not None
    num_peers = max(branch_map) + 1

    with SandboxMultiBranch(paths.TEZOS_BINARIES,
                            constants.IDENTITIES,
                            num_peers=num_peers,
                            log_dir=log_dir,
                            branch_map=branch_map) as sandbox:
        yield sandbox
        # this assertion checks that daemons (baker, endorser, node...) didn't
        # fail unexpected.
        assert sandbox.are_daemons_alive(), DEAD_DAEMONS_WARN


def pytest_collection_modifyitems(config, items):
    '''Adapted from pytest-fixture-marker: adds the regression marker
    to all tests that use the regtest fixture.
    '''
    # pylint: disable=unused-argument

    for item in items:
        if 'regtest' in item.fixturenames:
            item.add_marker('regression')


@pytest.fixture(scope="class")
def legacy_stores(request):
    """ Aims to generate legacy stores.

    The number of blocks to bake (batch), the home path and the
    export_snapshots variables are pecified as a class annotation.
    @pytest.mark.parametrize('legacy_stores', […], indirect=True)
    """
    assert request.param is not None
    home = request.param['home']
    batch = request.param['batch']
    export_snapshots = request.param['snapshot']
    session = {}
    data_dir = tempfile.mkdtemp(prefix='tezos-legacy-stores.')
    build_dir = f'_build/'
    builder_target = 'legacy_store_builder'
    builder_path = f'src/lib_store/legacy_store/{builder_target}.exe'
    builder_bin = f'{build_dir}default/{builder_path}'
    maker_target = 'legacy_store_maker'
    maker_path = f'src/lib_store/test/{maker_target}.exe'
    maker_bin = f'{build_dir}default/{maker_path}'
    print()
    subprocess.run(['dune', 'build', '--build-dir', f'{home}{build_dir}',
                    f'{builder_path}'],
                   check=True, cwd=home)
    subprocess.run(['dune', 'build', '--build-dir', f'{home}{build_dir}',
                    f'{maker_path}'],
                   check=True, cwd=home)
    # Call the magic binary wich generate legacy stores such as:
    # data_dir/archive_store_to_upgrade
    #         /full_store_to_upgrade
    #         /rolling_store_to_upgrade
    # where every store cointains the "same chain"
    subprocess.run([maker_bin, data_dir, builder_bin,
                    str(batch), str(export_snapshots).lower()],
                   check=True, cwd=home)

    # Store data paths in session
    for history_mode in ['archive', 'full', 'rolling']:
        path = f'{data_dir}/{history_mode}_store_to_upgrade'
        session[f'{history_mode}_path'] = path

    # Store snapshot paths in legacy_stores
    if export_snapshots:
        for history_mode in ['archive', 'full']:
            full_path = f'{data_dir}/snapshot_from_{history_mode}_storage.full'
            session[f'from_{history_mode}.full'] = full_path
            print(full_path)
            rolling_path = (f'{data_dir}' +
                            f'/snapshot_from_{history_mode}_storage.rolling')
            session[f'from_{history_mode}.rolling'] = rolling_path
            print(rolling_path)
        # Store the rolling path
        session[f'from_rolling.rolling'] = (f'{data_dir}/snapshot_from' +
                                            '_rolling_storage.rolling')

    yield session
    shutil.rmtree(data_dir)


@pytest.fixture(scope="class")
def nodes_legacy_store(sandbox, legacy_stores):
    nodes = {}

    # TODO would be cleaner to return couples (node, client) in order to
    #      avoid relying on the invariant that nodes are numbered 1, 2, 3
    #      or just return the id?
    i = 1
    for history_mode in ['archive', 'full', 'rolling']:
        node_dir = legacy_stores[f'{history_mode}_path']
        # init config with up to date version
        params = constants.NODE_PARAMS + ['--history-mode', history_mode]
        node = sandbox.register_node(i, node_dir=node_dir,
                                     params=params)
        # Workaround to allow generating an identity on an
        # old 0.0.4 storage with a 0.0.5 node
        version = open(node_dir+"/version.json", "w")
        version.write('{ "version": "0.0.5" }')
        version.close()
        node.init_config()
        # write version to upgrade
        version = open(node_dir+"/version.json", "w")
        version.write('{ "version": "0.0.4" }')
        version.close()

        nodes[history_mode] = node
        i += 1

    yield nodes

    # TODO think of case of failure before `yield`
    for history_mode in ['archive', 'full', 'rolling']:
        node_dir = legacy_stores[f'{history_mode}_path']
        shutil.rmtree(node_dir)

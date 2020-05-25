import pytest
from tools import utils, paths
from client import client_output

BAKE_ARGS = ['--max-priority', '512', '--minimal-timestamp']
BATCH = 100

EXPECTED_LEVEL = 101

EXPECTED_CHECKPOINT = 81  # checkpoint = lafl(head)
EXPECTED_SAVEPOINT = 81  # savepoint = checkpoint (legacy's Full limitations)
EXPECTED_CABOOSE = 0
# savepoint - maxopttl(cp)
EXPECTED_ROLLING_CABOOSE = EXPECTED_SAVEPOINT - 60

EXPECTED_SERVICE_ERROR = 'Did not find service'
EXPECTED_COMMAND_ERROR = 'Command failed : Unable to find block'


def check_expected_values(head):
    assert head['header']['level'] == EXPECTED_LEVEL


def restart(sandbox, node_id):
    sandbox.node(node_id).run()
    assert sandbox.client(node_id).check_node_listening()


def expect_wrong_version(sandbox, node):
    pattern = "Found '0.0.4', expected '0.0.5'"
    with utils.assert_run_failure(pattern):
        sandbox.init_node(node, snapshot=None, reconstruct=False,
                          legacy=False)


# @pytest.fixture(scope="class")
# def legacy_stores():
#     session = {}
#     data_dir = tempfile.mkdtemp(prefix='tezos-legacy-stores.')
#     build_dir = f'_build/'
#     builder_target = 'legacy_store_builder'
#     builder_path = f'src/lib_store/legacy_store/{builder_target}.exe'
#     builder_bin = f'{build_dir}default/{builder_path}'
#     maker_target = 'legacy_store_maker'
#     maker_path = f'src/lib_store/test/{maker_target}.exe'
#     maker_bin = f'{build_dir}default/{maker_path}'
#     print()
#     subprocess.run(['dune', 'build', '--build-dir', f'{HOME}{build_dir}',
#                     f'{builder_path}'],
#                    check=True, cwd=HOME)
#     subprocess.run(['dune', 'build', '--build-dir', f'{HOME}{build_dir}',
#                     f'{maker_path}'],
#                    check=True, cwd=HOME)
#     # Call the magic binary wich generate legacy stores such as:
#     # data_dir/archive_store_to_upgrade
#     #         /full_store_to_upgrade
#     #         /rolling_store_to_upgrade
#     # where every store cointains the "same chain"
#     export_snapshots = 'false'
#     subprocess.run([maker_bin, data_dir, builder_bin,
#                     str(BATCH), export_snapshots], check=True, cwd=HOME)
#     # Store data paths in session
#     for history_mode in ['archive', 'full', 'rolling']:
#         path = f'{data_dir}/{history_mode}_store_to_upgrade'
#         session[f'{history_mode}_path'] = path
#     yield session
#     shutil.rmtree(data_dir)


# @pytest.fixture(scope="class")
# def nodes_legacy_store(sandbox, legacy_stores):
#     nodes = {}

#     # TODO would be cleaner to return couples (node, client) in order to
#     #      avoid relying on the invariant that nodes are numbered 1, 2, 3
#     #      or just return the id?
#     i = 1
#     for history_mode in ['archive', 'full', 'rolling']:
#         node_dir = legacy_stores[f'{history_mode}_path']
#         # init config with up to date version
#         params = constants.NODE_PARAMS + ['--history-mode', history_mode]
#         node = sandbox.register_node(i, node_dir=node_dir,
#                                      params=params)
#         # Workaround to allow generating an identity on an
#         # old 0.0.4 storage with a 0.0.5 node
#         version = open(node_dir+"/version.json", "w")
#         version.write('{ "version": "0.0.5" }')
#         version.close()
#         node.init_config()
#         # write version to upgrade
#         version = open(node_dir+"/version.json", "w")
#         version.write('{ "version": "0.0.4" }')
#         version.close()

#         nodes[history_mode] = node
#         i += 1

#     yield nodes

#     # TODO think of case of failure before `yield`
#     for history_mode in ['archive', 'full', 'rolling']:
#         node_dir = legacy_stores[f'{history_mode}_path']
#         shutil.rmtree(node_dir)

MAP = {
    "batch": BATCH,
    "home": paths.TEZOS_HOME,
    "snapshot": False,
    }


@pytest.mark.incremental
@pytest.mark.snapshot
@pytest.mark.slow
class TestLegacy:

    # ARCHIVE
    @pytest.mark.parametrize("legacy_stores", [MAP], indirect=True)
    def test_upgrade_archive(self, sandbox, nodes_legacy_store):
        node1 = nodes_legacy_store['archive']
        # We init the client
        client1 = sandbox.register_client(1, rpc_port=node1.rpc_port)
        expect_wrong_version(sandbox, node1)
        # We now run the storage upgarde
        sandbox.node(1).upgrade_storage()
        # After upgrading, we restart the node
        restart(sandbox, 1)
        sandbox.init_client(client1)

    # Checkpoints
    def test_archive_consistency_1(self, sandbox):
        check_expected_values(sandbox.client(1).get_head())
        assert sandbox.client(1).get_savepoint() == 0
        assert sandbox.client(1).get_caboose() == 0

    # All blocks must be available
    def test_archive_consistency_2(self, sandbox):
        for i in range(EXPECTED_LEVEL):
            assert utils.get_block_at_level(sandbox.client(1), i)

    # FULL
    def test_upgrade_full(self, sandbox, nodes_legacy_store):
        node2 = nodes_legacy_store['full']
        # We init the client
        client2 = sandbox.register_client(2, rpc_port=node2.rpc_port)
        expect_wrong_version(sandbox, node2)
        # We now run the storage upgarde
        sandbox.node(2).upgrade_storage()
        # After upgrading, we restart the node
        restart(sandbox, 2)
        sandbox.init_client(client2)

    # Checkpoints
    def test_full_consistency_1(self, sandbox):
        check_expected_values(sandbox.client(2).get_head())
        savepoint = sandbox.client(2).get_savepoint()
        assert savepoint == EXPECTED_SAVEPOINT
        caboose = sandbox.client(2).get_caboose()
        assert caboose == 0
        # the metadata of genesis are available
        assert utils.get_block_at_level(sandbox.client(2), 0)

    # All block in [1; CHECKPOINT] must non be available (only headers are)
    def test_full_consistency_2(self, sandbox):
        for i in range(1, EXPECTED_CHECKPOINT):
            with pytest.raises(client_output.InvalidClientOutput) as exc:
                utils.get_block_metadata_at_level(sandbox.client(2), i)
            assert exc.value.client_output.startswith(EXPECTED_COMMAND_ERROR)

    # All block headers in [1; CHECKPOINT] must be available
    def test_full_consistency_3(self, sandbox):
        for i in range(1, EXPECTED_CHECKPOINT):
            utils.get_block_header_at_level(sandbox.client(2), i)

    # All blocks in [CHECKPOINT + 1; HEAD] must be available
    def test_full_consistency_4(self, sandbox):
        for i in range(EXPECTED_CHECKPOINT + 1, EXPECTED_LEVEL):
            assert utils.get_block_at_level(sandbox.client(2), i)

    # ROLLING
    def test_upgrade_rolling(self, sandbox, nodes_legacy_store):
        node3 = nodes_legacy_store['rolling']
        # We init the client
        client3 = sandbox.register_client(3, rpc_port=node3.rpc_port)
        expect_wrong_version(sandbox, node3)
        # We now run the storage upgarde
        sandbox.node(3).upgrade_storage()
        # After upgrading, we restart the node
        restart(sandbox, 3)
        sandbox.init_client(client3)

    # Checkpoints
    def test_rolling_consistency_1(self, sandbox):
        check_expected_values(sandbox.client(3).get_head())
        savepoint = sandbox.client(3).get_savepoint()
        assert savepoint == EXPECTED_CHECKPOINT
        # In rolling, caboose = savepoint
        caboose = sandbox.client(3).get_caboose()
        assert caboose == EXPECTED_ROLLING_CABOOSE
        # the metadata of genesis are available
        utils.get_block_at_level(sandbox.client(3), 0)

    # All blocks in [1 ; ROLLING_CABOOSE] must not be known
    def test_rolling_consistency_2(self, sandbox):
        for i in range(1, EXPECTED_ROLLING_CABOOSE):
            with utils.assert_run_failure(EXPECTED_SERVICE_ERROR):
                utils.get_block_at_level(sandbox.client(3), i)

    # All blocks in [ROLLING_CABOOSE ; CHECKPOINT] must not be available
    # (only headers are)
    def test_rolling_consistency_3(self, sandbox):
        for i in range(EXPECTED_ROLLING_CABOOSE, EXPECTED_CHECKPOINT):
            with pytest.raises(client_output.InvalidClientOutput) as exc:
                utils.get_block_metadata_at_level(sandbox.client(3), i)
            assert exc.value.client_output.startswith(EXPECTED_COMMAND_ERROR)

    # All block headers in [SAVEPOINT ; CHECKPOINT] must be available
    def test_rolling_consistency_4(self, sandbox):
        for i in range(EXPECTED_SAVEPOINT, EXPECTED_CHECKPOINT):
            utils.get_block_header_at_level(sandbox.client(3), i)

    # All blocks in [CHECKPOINT + 1; HEAD] must be available
    def test_rolling_consistency_5(self, sandbox):
        for i in range(EXPECTED_CHECKPOINT+1, EXPECTED_LEVEL):
            assert utils.get_block_at_level(sandbox.client(3), i)

    # Bake a few blocks to check if the Full and Rolling nodes catch up
    def test_bake_to_catch_up(self, sandbox):
        for _ in range(BATCH):
            sandbox.client(1).bake('bootstrap1', BAKE_ARGS)

    def test_archive_catch_up(self, sandbox):
        head = sandbox.client(1).get_head()
        expected_head = EXPECTED_LEVEL + BATCH
        assert head['header']['level'] == expected_head
        checkpoint = (sandbox.client(1).get_checkpoint())['block']['level']
        assert checkpoint == (expected_head - 2*8)
        savepoint = sandbox.client(1).get_savepoint()
        caboose = sandbox.client(1).get_caboose()
        assert savepoint == caboose
        assert caboose == 0

    # We assume that "Full 0 mode" is now in "Full 5 mode"
    def test_full_catch_up(self, sandbox):
        head = sandbox.client(2).get_head()
        expected_head = EXPECTED_LEVEL + BATCH
        assert head['header']['level'] == expected_head
        checkpoint = sandbox.client(2).get_checkpoint()['block']['level']
        assert checkpoint == (expected_head - 2*8)
        savepoint = sandbox.client(2).get_savepoint()
        assert savepoint == (checkpoint - 60)
        caboose = sandbox.client(2).get_caboose()
        assert caboose == 0

    # We assume that "Rolling 0 mode" is now in "Rolling 5 mode"
    def test_rolling_catch_up(self, sandbox):
        head = sandbox.client(3).get_head()
        expected_head = EXPECTED_LEVEL + BATCH
        assert head['header']['level'] == expected_head
        checkpoint = sandbox.client(3).get_checkpoint()['block']['level']
        assert checkpoint == (expected_head - 2*8)
        savepoint = sandbox.client(3).get_savepoint()
        assert savepoint == (checkpoint - 60)
        caboose = sandbox.client(3).get_caboose()
        assert caboose == savepoint

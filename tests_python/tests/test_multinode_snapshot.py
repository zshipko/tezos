import tempfile
import shutil
from datetime import datetime, timedelta
import pytest
from tools import utils, constants

BAKE_ARGS = ['--max-priority', '512', '--minimal-timestamp']
PARAMS = constants.NODE_PARAMS

BATCH_1 = 48
BATCH_2 = 48  # not enough bakes to drag the savepoint after snapshot import
BATCH_3 = 32  # enough bakes to drag the savepoint
GROUP1 = [0, 1, 2]
GROUP2 = [0, 1, 2, 3, 4, 5]
GROUP_FULL = [1, 3]
GROUP_ROLLING = [2, 4, 5]
SNAPSHOT_DIR = tempfile.mkdtemp(prefix='tezos-snapshots.')


def clean(node):
    shutil.rmtree(node.node_dir)


# Restart node. Side effect: clean store caches
def restart(sandbox, node_id):
    sandbox.node(node_id).terminate_or_kill()
    sandbox.node(node_id).run()
    assert sandbox.client(node_id).check_node_listening()


@pytest.mark.multinode
@pytest.mark.incremental
@pytest.mark.snapshot
@pytest.mark.slow
class TestMultiNodeSnapshot:
    # Tests both the snapshot mechanism and the store's behaviour
    # TL;DR, how it works:
    # - bake few blocks using all history modes
    # - export all kinds of snapshots
    # - import all kinds of snapshots
    # - check consistency (the snapshot's window includes genesis)
    # - bake a few blocks
    # - check consistency (the checkpoints should not move yet)
    # - bake a few blocks
    # - check consistency (the checkpoints should have moved)
    # - export all kinds of snapshots
    # - import all kinds of snapshots
    # - check consistency (checkpoints should be still valid)
    # - bake a few blocks
    # - check consistency (the checkpoints should have moved)

    def test_init(self, sandbox):
        # Node 0: archive baker
        sandbox.add_node(0, params=PARAMS + ['--history-mode', 'archive'])
        # Node 1: full
        sandbox.add_node(1, params=PARAMS)
        # Node 2: rolling
        sandbox.add_node(2, params=PARAMS + ['--history-mode', 'rolling'])
        # Allow fast `bake for` by activating the protocol in the past
        last_hour_date_time = datetime.utcnow() - timedelta(hours=1)
        timestamp = last_hour_date_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        utils.activate_alpha(sandbox.client(0), timestamp=timestamp)

    def test_bake_batch_1(self, sandbox, session):
        for _ in range(BATCH_1):
            sandbox.client(0).bake('bootstrap1', BAKE_ARGS)
            sandbox.client(0).endorse('bootstrap2')
        session['head_hash'] = sandbox.client(0).get_head()['hash']
        session['head_level'] = sandbox.client(0).get_head()['header']['level']
        session['snapshot_level'] = session['head_level']

    def test_group1_batch_1(self, sandbox, session):
        for i in GROUP1:
            assert utils.check_level(sandbox.client(i), session['head_level'])

    ###########################################################################
    # Export all kinds of snapshots
    def test_archive_export_full_1(self, sandbox, session):
        file = f'{SNAPSHOT_DIR}/node0_batch_1.full'
        export_level = session['snapshot_level']
        sandbox.node(0).snapshot_export(file,
                                        params=['--block', f'{export_level}'])

    def test_archive_export_rolling_1(self, sandbox, session):
        file = f'{SNAPSHOT_DIR}/node0_batch_1.rolling'
        export_level = session['snapshot_level']
        sandbox.node(0).snapshot_export(file,
                                        params=['--block', f'{export_level}',
                                                '--rolling'])

    def test_full_export_full_1(self, sandbox, session):
        file = f'{SNAPSHOT_DIR}/node1_batch_1.full'
        export_level = session['snapshot_level']
        sandbox.node(1).snapshot_export(file,
                                        params=['--block', f'{export_level}'])

    def test_full_export_rolling_1(self, sandbox, session):
        file = f'{SNAPSHOT_DIR}/node1_batch_1.rolling'
        export_level = session['snapshot_level']
        sandbox.node(1).snapshot_export(file,
                                        params=['--block', f'{export_level}',
                                                '--rolling'])

    def test_rolling_export_rolling_1(self, sandbox, session):
        file = f'{SNAPSHOT_DIR}/node2_batch_1.rolling'
        export_level = session['snapshot_level']
        sandbox.node(2).snapshot_export(file,
                                        params=['--block', f'{export_level}',
                                                '--rolling'])

    ###########################################################################
    # Import all kinds of snapshots
    # New node: 3
    def test_run_full_node_from_archive_1(self, sandbox):
        file = f'{SNAPSHOT_DIR}/node0_batch_1.full'
        sandbox.add_node(3, snapshot=file)

    # New node: 4
    def test_run_rolling_node_from_archive_1(self, sandbox):
        file = f'{SNAPSHOT_DIR}/node0_batch_1.rolling'
        sandbox.add_node(4, snapshot=file)

    # Reset node 1
    def test_reset_full_node_from_full_1(self, sandbox):
        file = f'{SNAPSHOT_DIR}/node1_batch_1.full'
        sandbox.rm_node(1)
        sandbox.add_node(1, snapshot=file)

    # New node: 5
    def test_run_rolling_node_from_full_1(self, sandbox):
        file = f'{SNAPSHOT_DIR}/node1_batch_1.rolling'
        sandbox.add_node(5, snapshot=file)

    # Reset node 2
    def test_reset_rolling_node_from_rolling_1(self, sandbox):
        file = f'{SNAPSHOT_DIR}/node2_batch_1.rolling'
        sandbox.rm_node(2)
        sandbox.add_node(2, snapshot=file)

    ###########################################################################
    # Check consistency of imported snapshots
    # Do not factorize calls to ease debugging
    # For the full nodes
    def test_node_1_consistency_1(self, sandbox, session):
        node_id = 1
        restart(sandbox, node_id)
        expected_level = session['snapshot_level']
        expected_checkpoint = expected_level
        expected_savepoint = expected_checkpoint
        expected_caboose = 0
        utils.node_consistency_after_import(node_id, sandbox,
                                            expected_level,
                                            expected_checkpoint,
                                            expected_savepoint,
                                            expected_caboose)
        utils.full_node_blocks_availability(node_id,
                                            sandbox,
                                            expected_savepoint,
                                            expected_level)

    def test_node_3_consistency_1(self, sandbox, session):
        node_id = 3
        restart(sandbox, node_id)
        expected_level = session['snapshot_level']
        expected_checkpoint = expected_level
        expected_savepoint = expected_checkpoint
        expected_caboose = 0
        utils.node_consistency_after_import(node_id, sandbox,
                                            expected_level,
                                            expected_checkpoint,
                                            expected_savepoint,
                                            expected_caboose)
        utils.full_node_blocks_availability(node_id,
                                            sandbox,
                                            expected_savepoint,
                                            expected_level)

    # For the rolling nodes
    def test_node_2_consistency_1(self, sandbox, session):
        node_id = 2
        restart(sandbox, node_id)
        expected_level = session['snapshot_level']
        expected_checkpoint = expected_level
        expected_savepoint = expected_checkpoint
        expected_caboose = 0
        utils.node_consistency_after_import(node_id, sandbox,
                                            expected_level,
                                            expected_checkpoint,
                                            expected_savepoint,
                                            expected_caboose)
        utils.rolling_node_blocks_availability(node_id,
                                               sandbox,
                                               expected_savepoint,
                                               expected_caboose,
                                               expected_level)

    def test_node_4_consistency_1(self, sandbox, session):
        node_id = 4
        restart(sandbox, node_id)
        expected_level = session['snapshot_level']
        expected_checkpoint = expected_level
        expected_savepoint = expected_checkpoint
        expected_caboose = 0
        utils.node_consistency_after_import(node_id, sandbox,
                                            expected_level,
                                            expected_checkpoint,
                                            expected_savepoint,
                                            expected_caboose)
        utils.rolling_node_blocks_availability(node_id,
                                               sandbox,
                                               expected_savepoint,
                                               expected_caboose,
                                               expected_level)

    def test_node_5_consistency_1(self, sandbox, session):
        node_id = 5
        restart(sandbox, node_id)
        expected_level = session['snapshot_level']
        expected_checkpoint = expected_level
        expected_savepoint = expected_checkpoint
        expected_caboose = 0
        utils.node_consistency_after_import(node_id, sandbox,
                                            expected_level,
                                            expected_checkpoint,
                                            expected_savepoint,
                                            expected_caboose)
        utils.rolling_node_blocks_availability(node_id,
                                               sandbox,
                                               expected_savepoint,
                                               expected_caboose,
                                               expected_level)

    ###########################################################################
    # Bake a few blocks
    def test_bake_batch_2(self, sandbox, session):
        for _ in range(BATCH_2):
            sandbox.client(0).bake('bootstrap1', BAKE_ARGS)
            sandbox.client(0).endorse('bootstrap2')
        session['head_hash'] = sandbox.client(0).get_head()['hash']
        session['head_level'] = sandbox.client(0).get_head()['header']['level']
        for i in GROUP2:
            assert utils.check_level(sandbox.client(i), session['head_level'])

    ###########################################################################
    # Check consistency of imported snapshots after > 5 baked cycles
    # The savepoints of full and rolling nodes **have not** been dragged yet

    # For the full nodes
    def test_node_1_consistency_2(self, sandbox, session):
        node_id = 1
        restart(sandbox, node_id)
        expected_level = session['head_level']
        expected_checkpoint = expected_level - 2 * 8  # lafl(head)
        savepoint_when_imported = session['snapshot_level']
        expected_savepoint = savepoint_when_imported
        expected_caboose = 0
        utils.node_consistency_after_import(node_id, sandbox,
                                            expected_level,
                                            expected_checkpoint,
                                            expected_savepoint,
                                            expected_caboose)
        utils.full_node_blocks_availability(node_id,
                                            sandbox,
                                            expected_savepoint,
                                            expected_level)

    def test_node_3_consistency_2(self, sandbox, session):
        node_id = 3
        restart(sandbox, node_id)
        expected_level = session['head_level']
        expected_checkpoint = expected_level - 2 * 8  # lafl(head)
        savepoint_when_imported = session['snapshot_level']
        expected_savepoint = savepoint_when_imported
        expected_caboose = 0
        utils.node_consistency_after_import(node_id, sandbox,
                                            expected_level,
                                            expected_checkpoint,
                                            expected_savepoint,
                                            expected_caboose)
        utils.full_node_blocks_availability(node_id,
                                            sandbox,
                                            expected_savepoint,
                                            expected_level)

    # For the rolling nodes
    # The caboose of rolling mode were no dragged yet as
    # (checkpoint - maxopttl(head)) < savepoint
    def test_node_2_consistency_2(self, sandbox, session):
        node_id = 2
        restart(sandbox, node_id)
        expected_level = session['head_level']
        expected_checkpoint = expected_level - 2 * 8  # lafl(head)
        savepoint_when_imported = session['snapshot_level']
        expected_savepoint = savepoint_when_imported
        head = sandbox.client(node_id).get_head()
        maxopttl = head['metadata']['max_operations_ttl']
        expected_caboose = expected_checkpoint - maxopttl
        utils.node_consistency_after_import(node_id, sandbox,
                                            expected_level,
                                            expected_checkpoint,
                                            expected_savepoint,
                                            expected_caboose)
        utils.rolling_node_blocks_availability(node_id,
                                               sandbox,
                                               expected_savepoint,
                                               expected_caboose,
                                               expected_level)

    def test_node_4_consistency_2(self, sandbox, session):
        node_id = 4
        restart(sandbox, node_id)
        expected_level = session['head_level']
        expected_checkpoint = expected_level - 2 * 8  # lafl(head)
        savepoint_when_imported = session['snapshot_level']
        expected_savepoint = savepoint_when_imported
        head = sandbox.client(node_id).get_head()
        maxopttl = head['metadata']['max_operations_ttl']
        expected_caboose = expected_checkpoint - maxopttl
        utils.node_consistency_after_import(node_id, sandbox,
                                            expected_level,
                                            expected_checkpoint,
                                            expected_savepoint,
                                            expected_caboose)
        utils.rolling_node_blocks_availability(node_id,
                                               sandbox,
                                               expected_savepoint,
                                               expected_caboose,
                                               expected_level)

    def test_node_5_consistency_2(self, sandbox, session):
        node_id = 5
        restart(sandbox, node_id)
        expected_level = session['head_level']
        expected_checkpoint = expected_level - 2 * 8  # lafl(head)
        savepoint_when_imported = session['snapshot_level']
        expected_savepoint = savepoint_when_imported
        head = sandbox.client(node_id).get_head()
        maxopttl = head['metadata']['max_operations_ttl']
        expected_caboose = expected_checkpoint - maxopttl
        utils.node_consistency_after_import(node_id, sandbox,
                                            expected_level,
                                            expected_checkpoint,
                                            expected_savepoint,
                                            expected_caboose)
        utils.rolling_node_blocks_availability(node_id,
                                               sandbox,
                                               expected_savepoint,
                                               expected_caboose,
                                               expected_level)

    ###########################################################################
    # Bake a few blocks
    def test_bake_batch_3(self, sandbox, session):
        for _ in range(BATCH_3):
            sandbox.client(0).bake('bootstrap1', BAKE_ARGS)
            sandbox.client(0).endorse('bootstrap2')
        session['head_hash'] = sandbox.client(0).get_head()['hash']
        session['head_level'] = sandbox.client(0).get_head()['header']['level']
        session['snapshot_level'] = session['head_level']
        for i in GROUP2:
            assert utils.check_level(sandbox.client(i), session['head_level'])

    ###########################################################################
    # Check consistency of imported snapshots after > 5 baked cycles
    # The savepoints of full and rolling nodes **have** been dragged yet

    # For the full nodes
    def test_node_1_consistency_3(self, sandbox, session):
        node_id = 1
        restart(sandbox, node_id)
        expected_level = session['head_level']
        expected_checkpoint = expected_level - 2 * 8  # lafl(head)
        head = sandbox.client(node_id).get_head()
        maxopttl = head['metadata']['max_operations_ttl']
        expected_savepoint = expected_checkpoint - maxopttl
        expected_caboose = 0
        utils.node_consistency_after_import(node_id, sandbox,
                                            expected_level,
                                            expected_checkpoint,
                                            expected_savepoint,
                                            expected_caboose)
        utils.full_node_blocks_availability(node_id,
                                            sandbox,
                                            expected_savepoint,
                                            expected_level)

    def test_node_3_consistency_3(self, sandbox, session):
        node_id = 3
        restart(sandbox, node_id)
        expected_level = session['head_level']
        expected_checkpoint = expected_level - 2 * 8  # lafl(head)
        head = sandbox.client(node_id).get_head()
        maxopttl = head['metadata']['max_operations_ttl']
        expected_savepoint = expected_checkpoint - maxopttl
        expected_caboose = 0
        utils.node_consistency_after_import(node_id, sandbox,
                                            expected_level,
                                            expected_checkpoint,
                                            expected_savepoint,
                                            expected_caboose)
        utils.full_node_blocks_availability(node_id,
                                            sandbox,
                                            expected_savepoint,
                                            expected_level)

    # For the rolling nodes
    def test_node_2_consistency_3(self, sandbox, session):
        node_id = 2
        restart(sandbox, node_id)
        expected_level = session['head_level']
        expected_checkpoint = expected_level - 2 * 8  # lafl(head)
        head = sandbox.client(node_id).get_head()
        maxopttl = head['metadata']['max_operations_ttl']
        expected_savepoint = expected_checkpoint - maxopttl
        expected_caboose = expected_savepoint
        utils.node_consistency_after_import(node_id, sandbox,
                                            expected_level,
                                            expected_checkpoint,
                                            expected_savepoint,
                                            expected_caboose)
        utils.rolling_node_blocks_availability(node_id,
                                               sandbox,
                                               expected_savepoint,
                                               expected_caboose,
                                               expected_level)

    def test_node_4_consistency_3(self, sandbox, session):
        node_id = 4
        restart(sandbox, node_id)
        expected_level = session['head_level']
        expected_checkpoint = expected_level - 2 * 8  # lafl(head)
        head = sandbox.client(node_id).get_head()
        maxopttl = head['metadata']['max_operations_ttl']
        expected_savepoint = expected_checkpoint - maxopttl
        expected_caboose = expected_savepoint
        utils.node_consistency_after_import(node_id, sandbox,
                                            expected_level,
                                            expected_checkpoint,
                                            expected_savepoint,
                                            expected_caboose)
        utils.rolling_node_blocks_availability(node_id,
                                               sandbox,
                                               expected_savepoint,
                                               expected_caboose,
                                               expected_level)

    def test_node_5_consistency_3(self, sandbox, session):
        node_id = 5
        restart(sandbox, node_id)
        expected_level = session['head_level']
        expected_checkpoint = expected_level - 2 * 8  # lafl(head)
        head = sandbox.client(node_id).get_head()
        maxopttl = head['metadata']['max_operations_ttl']
        expected_savepoint = expected_checkpoint - maxopttl
        expected_caboose = expected_savepoint
        utils.node_consistency_after_import(node_id, sandbox,
                                            expected_level,
                                            expected_checkpoint,
                                            expected_savepoint,
                                            expected_caboose)
        utils.rolling_node_blocks_availability(node_id,
                                               sandbox,
                                               expected_savepoint,
                                               expected_caboose,
                                               expected_level)

    ###########################################################################
    # Re-export all kinds of snapshots
    def test_archive_export_full_2(self, sandbox, session):
        file = f'{SNAPSHOT_DIR}/node0_batch_3.full'
        export_level = session['snapshot_level']
        sandbox.node(0).snapshot_export(file,
                                        params=['--block', f'{export_level}'])

    def test_archive_export_rolling_2(self, sandbox, session):
        file = f'{SNAPSHOT_DIR}/node0_batch_3.rolling'
        export_level = session['snapshot_level']
        sandbox.node(0).snapshot_export(file,
                                        params=['--block', f'{export_level}',
                                                '--rolling'])

    def test_full_export_full_2(self, sandbox, session):
        file = f'{SNAPSHOT_DIR}/node1_batch_3.full'
        export_level = session['snapshot_level']
        sandbox.node(1).snapshot_export(file,
                                        params=['--block', f'{export_level}'])

    def test_full_export_rolling_2(self, sandbox, session):
        file = f'{SNAPSHOT_DIR}/node1_batch_3.rolling'
        export_level = session['snapshot_level']
        sandbox.node(1).snapshot_export(file,
                                        params=['--block', f'{export_level}',
                                                '--rolling'])

    def test_rolling_export_rolling_2(self, sandbox, session):
        file = f'{SNAPSHOT_DIR}/node2_batch_3.rolling'
        export_level = session['snapshot_level']
        sandbox.node(2).snapshot_export(file,
                                        params=['--block', f'{export_level}',
                                                '--rolling'])

    ###########################################################################
    # Import all kinds of snapshots
    # Reset node: 3
    def test_run_full_node_from_archive_2(self, sandbox):
        file = f'{SNAPSHOT_DIR}/node0_batch_3.full'
        sandbox.rm_node(3)
        sandbox.add_node(3, snapshot=file)

    # Reset node: 4
    def test_run_rolling_node_from_archive_2(self, sandbox):
        file = f'{SNAPSHOT_DIR}/node0_batch_3.rolling'
        sandbox.rm_node(4)
        sandbox.add_node(4, snapshot=file)

    # Reset node 1
    def test_reset_full_node_from_full_2(self, sandbox):
        file = f'{SNAPSHOT_DIR}/node1_batch_3.full'
        sandbox.rm_node(1)
        sandbox.add_node(1, snapshot=file)

    # Reset node: 5
    def test_run_rolling_node_from_full_2(self, sandbox):
        file = f'{SNAPSHOT_DIR}/node1_batch_3.rolling'
        sandbox.rm_node(5)
        sandbox.add_node(5, snapshot=file)

    # Reset node 2
    def test_reset_rolling_node_from_rolling_2(self, sandbox):
        file = f'{SNAPSHOT_DIR}/node2_batch_3.rolling'
        sandbox.rm_node(2)
        sandbox.add_node(2, snapshot=file)

    ###########################################################################
    # Check consistency of imported snapshots with > 5 cycles

    # For the full nodes
    def test_node_1_consistency_4(self, sandbox, session):
        node_id = 1
        restart(sandbox, node_id)
        expected_level = session['head_level']
        expected_checkpoint = expected_level
        expected_savepoint = expected_checkpoint
        expected_caboose = 0
        utils.node_consistency_after_import(node_id, sandbox,
                                            expected_level,
                                            expected_checkpoint,
                                            expected_savepoint,
                                            expected_caboose)
        utils.full_node_blocks_availability(node_id,
                                            sandbox,
                                            expected_savepoint,
                                            expected_level)

    def test_node_3_consistency_4(self, sandbox, session):
        node_id = 3
        restart(sandbox, node_id)
        expected_level = session['head_level']
        expected_checkpoint = expected_level
        expected_savepoint = expected_checkpoint
        expected_caboose = 0
        utils.node_consistency_after_import(node_id, sandbox,
                                            expected_level,
                                            expected_checkpoint,
                                            expected_savepoint,
                                            expected_caboose)
        utils.full_node_blocks_availability(node_id,
                                            sandbox,
                                            expected_savepoint,
                                            expected_level)

    # For the rolling nodes
    def test_node_2_consistency_4(self, sandbox, session):
        node_id = 2
        restart(sandbox, node_id)
        expected_level = session['head_level']
        expected_checkpoint = expected_level
        head = sandbox.client(node_id).get_head()
        maxopttl = head['metadata']['max_operations_ttl']
        expected_savepoint = expected_checkpoint
        expected_caboose = expected_checkpoint - maxopttl
        utils.node_consistency_after_import(node_id, sandbox,
                                            expected_level,
                                            expected_checkpoint,
                                            expected_savepoint,
                                            expected_caboose)
        utils.rolling_node_blocks_availability(node_id,
                                               sandbox,
                                               expected_savepoint,
                                               expected_caboose,
                                               expected_level)

    def test_node_4_consistency_4(self, sandbox, session):
        node_id = 4
        restart(sandbox, node_id)
        expected_level = session['head_level']
        expected_checkpoint = expected_level
        head = sandbox.client(node_id).get_head()
        maxopttl = head['metadata']['max_operations_ttl']
        expected_savepoint = expected_checkpoint
        expected_caboose = expected_checkpoint - maxopttl
        utils.node_consistency_after_import(node_id, sandbox,
                                            expected_level,
                                            expected_checkpoint,
                                            expected_savepoint,
                                            expected_caboose)
        utils.rolling_node_blocks_availability(node_id,
                                               sandbox,
                                               expected_savepoint,
                                               expected_caboose,
                                               expected_level)

    def test_node_5_consistency_4(self, sandbox, session):
        node_id = 5
        restart(sandbox, node_id)
        expected_level = session['head_level']
        expected_checkpoint = expected_level
        head = sandbox.client(node_id).get_head()
        maxopttl = head['metadata']['max_operations_ttl']
        expected_savepoint = expected_checkpoint
        expected_caboose = expected_checkpoint - maxopttl
        utils.node_consistency_after_import(node_id, sandbox,
                                            expected_level,
                                            expected_checkpoint,
                                            expected_savepoint,
                                            expected_caboose)
        utils.rolling_node_blocks_availability(node_id,
                                               sandbox,
                                               expected_savepoint,
                                               expected_caboose,
                                               expected_level)

    ###########################################################################
    # Clean exported snapshots

    def test_clean_files(self):
        shutil.rmtree(SNAPSHOT_DIR)

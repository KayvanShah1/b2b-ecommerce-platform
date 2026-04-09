from b2b_ec_pipeline.state.archive import MetadataArchiveWriter
from b2b_ec_pipeline.state.snapshot_manager import IngestionSnapshotManager
from b2b_ec_pipeline.state.state_manager import IngestionStateManager, StateResolver

archive_writer = MetadataArchiveWriter()
snapshot_manager = IngestionSnapshotManager(archive_writer=archive_writer)
state_manager = IngestionStateManager(snapshot_manager)
state_resolver = StateResolver(state_manager)

import time
import logging
from blockchainetl.jobs.event_exporter import ExportEventJob
from blockchainetl.streaming.streaming_exporter_creator import create_steaming_exporter

_LOGGER = logging.getLogger(__name__)


def export_events(output, logs, chain):
    """Exports blocks and transactions."""
    if output is None:
        raise ValueError('Either --blocks-output or --transactions-output options must be provided')
    start_time = time.time()
    _LOGGER.info(f"Start crawl data")
    job = ExportEventJob(
        chain=chain,
        logs=logs,
        item_exporter=output,
    )
    job.run()
    _LOGGER.info(f"crawl data in {time.time() - start_time}")
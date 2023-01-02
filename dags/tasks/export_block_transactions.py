import time
import logging
from blockchainetl.jobs.block_transaction_exporter import ExportBlocksJob
from blockchainetl.providers.auto import get_provider_from_uri
from blockchainetl.streaming.streaming_exporter_creator import create_steaming_exporter
from blockchainetl.utils.thread_local_proxy import ThreadLocalProxy

_LOGGER = logging.getLogger(__name__)


def export_blocks_transactions(start_block, end_block, provider_uri,
                               output, chain, batch_size=400, max_workers=4,
                               export_block=False, export_tx=True):
    """Exports blocks and transactions."""
    if output is None:
        raise ValueError('Either --blocks-output or --transactions-output options must be provided')

    start_time = time.time()
    _LOGGER.info(f"Start crawl data")
    job = ExportBlocksJob(
        chain=chain,
        start_block=start_block,
        end_block=end_block,
        batch_size=batch_size,
        batch_web3_provider=ThreadLocalProxy(lambda: get_provider_from_uri(provider_uri, batch=True)),
        max_workers=max_workers,
        item_exporter=output,
        export_blocks=export_block,
        export_transactions=export_tx)
    job.run()
    _LOGGER.info(f"crawl data in {time.time() - start_time}")
    return job.get_txs()

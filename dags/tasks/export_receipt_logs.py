import time
import logging
from blockchainetl.jobs.receipt_log_exporter import ExportReceiptsJob
from blockchainetl.providers.auto import get_provider_from_uri
from blockchainetl.streaming.streaming_exporter_creator import create_steaming_exporter
from blockchainetl.utils.thread_local_proxy import ThreadLocalProxy

_LOGGER = logging.getLogger(__name__)


def export_receipt_logs(
        provider_uri, output, chain, transaction_hashes,
        batch_size=400, max_workers=4, export_receipts=False, export_logs=True):
    """Exports blocks and transactions."""
    if output is None:
        raise ValueError('Either --blocks-output or --transactions-output options must be provided')
    start_time = time.time()
    _LOGGER.info(f"Start crawl data")
    job = ExportReceiptsJob(
        chain=chain,
        batch_size=batch_size,
        batch_web3_provider=ThreadLocalProxy(lambda: get_provider_from_uri(provider_uri, batch=True)),
        max_workers=max_workers,
        item_exporter=output,
        transaction_hashes_iterable=transaction_hashes,
        export_receipts=export_receipts,
        export_logs=export_logs
    )
    job.run()
    _LOGGER.info(f"crawl data in {time.time() - start_time}")
    return job.get_logs()

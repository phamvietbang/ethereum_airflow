import click
import time
import logging

from configs.blockchain_etl_config import BlockchainEtlConfig
from blockchainetl.jobs.stream_exporter import StreamExporter
from blockchainetl.streaming.streamer import Streamer
from blockchainetl.providers.auto import get_provider_from_uri
from blockchainetl.streaming.streaming_exporter_creator import create_steaming_exporter
from blockchainetl.utils.thread_local_proxy import ThreadLocalProxy

_LOGGER = logging.getLogger(__name__)


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-s', '--start-block', default=None, show_default=True, type=int, help='Start block')
@click.option('-e', '--end-block', default=None, type=int, help='End block')
@click.option('-b', '--batch-size', default=100, show_default=True, type=int,
              help='The number of blocks to export at a time.')
@click.option('-B', '--stream-batch-size', default=400, show_default=True, type=int,
              help='The number of blocks to export at a time.')
@click.option('-p', '--provider-uri', default='https://mainnet.infura.io', show_default=True, type=str,
              help='The URI of the web3 provider e.g. '
                   'file://$HOME/Library/Ethereum/geth.ipc or https://mainnet.infura.io')
@click.option('-w', '--max-workers', default=4, show_default=True, type=int, help='The maximum number of workers.')
@click.option('--output', default=None, show_default=True, type=str,
              help='The output file for transactions. '
                   'If not provided transactions will not be exported. Use "-" for stdout')
@click.option('-c', '--chain', default='bsc', show_default=True, type=str, help='The chain network to connect to.')
@click.option('-l', '--last-block-file', default="last_block_crawled.txt",
              show_default=True, type=str, help='The last block crawled')
@click.option('--lag', default=1, show_default=True,
              type=int, help='The number of block before the latest block')
@click.option('--period-seconds', default=1, show_default=True, type=int,
              help='How many seconds to sleep between syncs')
@click.option('--export-blocks', default=False, show_default=True, type=bool, help='Save block data or not')
@click.option('--export-transactions', default=True, show_default=True, type=bool, help='Save transaction or not')
@click.option('--export-receipts', default=False, show_default=True, type=bool, help='Save receipts or not')
@click.option('--export-logs', default=True, show_default=True, type=bool, help='Save logs or not')
@click.option('--decoded-logs', default=[], show_default=True, type=str, help='decode logs', multiple=True)
def eth_stream(start_block, end_block, batch_size, stream_batch_size, provider_uri, max_workers,
               output, chain, last_block_file, lag, period_seconds,
               export_blocks, export_transactions, export_receipts,
               export_logs, decoded_logs):
    """Exports blocks and transactions."""
    if output is None:
        raise ValueError('--output options must be provided')
    item = create_steaming_exporter(output=output)
    start_time = time.time()
    _LOGGER.info(f"Start crawl data")
    job = StreamExporter(
        chain=chain,
        batch_size=batch_size,
        batch_web3_provider=ThreadLocalProxy(lambda: get_provider_from_uri(provider_uri, batch=True)),
        max_workers=max_workers,
        item_exporter=item,
        export_blocks=export_blocks,
        export_transactions=export_transactions,
        export_logs=export_logs,
        export_receipts=export_receipts,
        decode_logs=decoded_logs
    )
    streamer = Streamer(
        chain=chain,
        blockchain_streamer_adapter=job,
        last_synced_block_file=last_block_file,
        lag=lag,
        start_block=start_block,
        end_block=end_block,
        period_seconds=period_seconds,
        block_batch_size=stream_batch_size,
        retry_errors=True,
        pid_file=None,
        stream_id=last_block_file,
        output=BlockchainEtlConfig.OUTPUT,
    )
    streamer.stream()
    _LOGGER.info(f"crawl data in {time.time() - start_time}")

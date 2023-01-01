import logging

from web3 import Web3
from web3.middleware import geth_poa_middleware

from blockchainetl.jobs.block_transaction_exporter import ExportBlocksJob
from blockchainetl.jobs.receipt_log_exporter import ExportReceiptsJob
from blockchainetl.jobs.log_exporter import ExportLogJob
from blockchainetl.jobs.event_exporter import ExportEventJob

_LOGGER = logging.getLogger(__name__)


class StreamExporter:
    def __init__(self,
                 chain,
                 batch_size,
                 batch_web3_provider,
                 max_workers,
                 item_exporter,
                 export_receipts=True,
                 export_logs=True,
                 export_blocks=True,
                 export_transactions=True,
                 decode_logs=None
                 ):
        self.export_transactions = export_transactions
        self.export_blocks = export_blocks
        self.item_exporter = item_exporter
        self.max_workers = max_workers
        self.batch_web3_provider = batch_web3_provider
        self.w3 = Web3(batch_web3_provider)
        self.w3.middleware_onion.inject(geth_poa_middleware, layer=0)
        self.batch_size = batch_size
        self.chain = chain
        self.decode_logs = decode_logs
        self.export_receipts = export_receipts
        self.export_logs = export_logs

    def open(self):
        self.item_exporter.open()

    def get_current_block_number(self):
        block_number = self.w3.eth.blockNumber
        return int(block_number)

    def export_all(self, start_block, end_block):
        _LOGGER.info(f'Start crawl data from {start_block} to {end_block}')
        transactions, logs = None, None
        if self.export_blocks or self.export_transactions:
            job = ExportBlocksJob(
                chain=self.chain,
                start_block=start_block,
                end_block=end_block,
                batch_size=self.batch_size,
                batch_web3_provider=self.batch_web3_provider,
                max_workers=self.max_workers,
                item_exporter=self.item_exporter,
                export_blocks=self.export_blocks,
                export_transactions=self.export_transactions
            )
            job.run()
            transactions = job.get_txs()

        if self.export_logs or self.export_receipts:
            if transactions:
                transaction_hashes_iterable = [i["hash"] for i in transactions]
                job = ExportReceiptsJob(
                    chain=self.chain,
                    transaction_hashes_iterable=transaction_hashes_iterable,
                    batch_size=self.batch_size,
                    batch_web3_provider=self.batch_web3_provider,
                    max_workers=self.max_workers,
                    item_exporter=self.item_exporter,
                    export_receipts=self.export_receipts,
                    export_logs=self.export_logs,
                )
                job.run()
                logs = job.get_logs()
            else:
                job = ExportLogJob(
                    chain=self.chain,
                    start_block=start_block,
                    end_block=end_block,
                    batch_size=self.batch_size,
                    max_workers=self.max_workers,
                    item_exporter=self.item_exporter,
                    provider=self.batch_web3_provider,
                    contract_addresses=None
                )
                job.run()
                logs = job.get_logs()

        if 'transfer' in self.decode_logs and logs:
            job = ExportEventJob(
                chain=self.chain,
                logs=logs,
                item_exporter=self.item_exporter,
            )
            job.run()

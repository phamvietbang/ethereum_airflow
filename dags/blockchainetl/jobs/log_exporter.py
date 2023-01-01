from blockchainetl.mappers.receipt_log_mapper import EthReceiptLogMapper
import logging
from blockchainetl.executors.batch_work_executor import BatchWorkExecutor
from blockchainetl.jobs.base_job import BaseJob
from web3 import Web3
from web3.middleware import geth_poa_middleware

_LOGGER = logging.getLogger(__name__)


class ExportLogJob(BaseJob):
    def __init__(self,
                 chain,
                 start_block,
                 end_block,
                 batch_size,
                 max_workers,
                 item_exporter,
                 provider,
                 contract_addresses=None):
        self.chain = chain
        self.web3 = Web3(provider)
        self.web3.middleware_onion.inject(geth_poa_middleware, layer=0)
        self.item_exporter = item_exporter
        self.start_block = start_block
        self.end_block = end_block
        self.batch_work_executor = BatchWorkExecutor(batch_size, max_workers)
        self.receipt_log = EthReceiptLogMapper()
        self.contract_addresses = contract_addresses
        self.logs = []

    def get_logs(self):
        return self.logs

    def _start(self):
        self.item_exporter.open()
        _LOGGER.info(f'start crawl events')

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.export_items(self.chain, "logs", self.logs)
        self.item_exporter.close()
        _LOGGER.info(f'Crawled events from {self.start_block} to {self.end_block}!')

    def _export(self):
        self.batch_work_executor.execute(
            range(self.start_block, self.end_block + 1),
            self.export_batch
        )

    def export_batch(self, block_number_batch):
        _LOGGER.info(f'crawling event data from {block_number_batch[0]} to {block_number_batch[-1]}')
        # for abi in self.list_abi:
        self.export_events(block_number_batch[0], block_number_batch[-1], pools=self.contract_addresses)

    def export_events(self, start_block, end_block, pools=None):
        filter_params = {
            'fromBlock': start_block,
            'toBlock': end_block
        }
        if pools is not None and len(pools) > 0:
            filter_params['address'] = pools

        log_filter = self.web3.eth.filter(filter_params)
        logs = log_filter.get_all_entries()
        for log in logs:
            log = self.receipt_log.web3_dict_to_receipt_log(log)
            log = self.receipt_log.receipt_log_to_dict(log)
            if log not in self.logs:
                self.logs.append(log)

        self.web3.eth.uninstallFilter(log_filter.filter_id)

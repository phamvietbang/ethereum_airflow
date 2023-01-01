from data_storage.memory_storage import MemoryStorage
import logging
from blockchainetl.jobs.base_job import BaseJob
from blockchainetl.mappers.event_mapper import EthEventMapper
from constants.event_constants import Event
from artifacts.abi.events.transfer_event_abi import TRANSFER_EVENT_ABI

_LOGGER = logging.getLogger(__name__)


class ExportEventJob(BaseJob):
    def __init__(self,
                 chain,
                 logs,
                 item_exporter,
                 abi=TRANSFER_EVENT_ABI):
        self.logs = logs
        self.chain = chain
        self.abi = abi
        self.item_exporter = item_exporter
        self.receipt_log = EthEventMapper()
        self.localstorage = MemoryStorage.getInstance()
        self.event_info = {}
        self.topics = []

    def _start(self):
        self.events = []
        self.decode_event_information_list = self.receipt_log.get_decode_event_information_list(self.abi)

        for decode_event_information in self.decode_event_information_list:
            self.event_info[decode_event_information.topic_hash] = decode_event_information
            self.topics.append(decode_event_information.topic_hash)

        self.item_exporter.open()
        _LOGGER.info(f'start crawl events')

    def _end(self):
        self.item_exporter.close()
        _LOGGER.info(f'Decode {len(self.events)} events from {len(self.logs)} logs!')

    def _export(self):
        _LOGGER.info(f'Decoding event data from logs')
        for log in self.logs:
            if log['topics'][0] in self.topics:
                event_dict = self.receipt_log.extract_event_from_log(log, self.event_info[log['topics'][0]])
                event_type = f'{event_dict[Event.event_type]}_event'.lower()
                del event_dict[Event.event_type]
                self.item_exporter.export_items(self.chain, event_type, [event_dict])
                self.events.append(event_dict)

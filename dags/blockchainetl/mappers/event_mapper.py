from eth_utils import keccak
from utils.utils import *
import logging
from blockchainetl.domain.lending_log_subcriber import EventSubscriber

logger = logging.getLogger('EthLendingService')


class EthEventMapper(object):

    def get_decode_event_information_list(self, abi):
        list_ = []
        for item in abi:
            arr = self.get_decode_event_infomation(item)
            if arr:
                list_.append(arr)
        return list_

    def get_decode_event_infomation(self, abi):
        event_abi = abi
        if event_abi.get('type') == 'event':
            event_hash = get_event_hash(event_abi)
            list_params = get_list_params(event_abi)
            event_name = event_abi.get('name')
            decode_event_information = EventSubscriber(event_hash, event_name, list_params)
            return decode_event_information
        return None

    def decode_data_by_type(self, data, type):
        if self.is_integers(type):
            return hex_to_dec(data)
        elif type == "address":
            return word_to_address(data)
        else:
            return data

    def is_integers(self, type):
        return type in {"uint256", "uint128", "uint64", "uint32", "uint16", "uint8", "uint"
            , "int256", "init128", "init64", "init32", "init16", "init8", "init"}

    def extract_event_from_log(self, receipt_log, event_subscriber):
        event = {}
        topics = receipt_log['topics']
        if topics is None or len(topics) < 1:
            logger.warning("Topics are empty in log {} of transaction {}".format(receipt_log['log_index'],
                                                                                 receipt_log['transaction_hash']))
            return None
        if event_subscriber.topic_hash == topics[0]:
            # Handle unindexed event fields
            topics_with_data = topics + split_to_words(receipt_log['data'])
            list_params_in_order = event_subscriber.list_params_in_order
            # if the number of topics and fields in data part != len(list_params_in_order), then it's a weird event
            num_params = len(list_params_in_order)
            topics_with_data = topics_with_data[1:]
            if len(topics_with_data) != num_params:
                logger.warning("The number of topics and data parts is not equal to {} in log {} of transaction {}"
                               .format(str(num_params), receipt_log['log_index'], receipt_log['transaction_hash']))
                return None

            event["event_type"] = event_subscriber.name
            event['transaction_has'] = receipt_log['transaction_hash']
            event["block_number"] = receipt_log["block_number"]
            event["log_index"] = receipt_log["log_index"]
            event["token_address"] = receipt_log["address"]
            for i in range(num_params):
                param_i = list_params_in_order[i]
                name = param_i.get("name")
                type = param_i.get("type")
                data = topics_with_data[i]
                event[name] = self.decode_data_by_type(data, type)
                if type == "uint256":
                    event[name] /= 10 ** 10
            return event

        return None


# remove redundancy in topic
def split_to_words(data):
    if data and len(data) > 2:
        data_without_0x = data[2:]
        words = list(chunk_string(data_without_0x, 64))
        words_with_0x = list(map(lambda word: '0x' + word, words))
        return words_with_0x
    return []


# convert topic to address
def word_to_address(param):
    if param is None:
        return None
    elif len(param) >= 40:
        return to_normalized_address('0x' + param[-40:])
    else:
        return to_normalized_address(param)


# hash abi to be topic
def get_event_hash(event_abi):
    input_string = event_abi.get("name") + "("
    for input in event_abi.get("inputs"):
        input_string += input.get("type") + ","
    input_string = input_string[:-1] + ")"
    hash = keccak(text=input_string)
    return '0x' + hash.hex()


# get params from abi
def get_list_params(event_abi):
    indexed_params, non_indexed_params = [], []
    for input in event_abi.get('inputs'):
        if input.get('indexed'):
            indexed_params.append(input)
        else:
            non_indexed_params.append(input)
    return indexed_params + non_indexed_params


def get_address_name_field(event_abi):
    address_name_field = []
    for input in event_abi.get('inputs'):
        if input.get('type') == 'address':
            address_name_field.append(input.get('name'))
    return address_name_field

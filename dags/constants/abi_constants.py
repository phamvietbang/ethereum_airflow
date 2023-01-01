from artifacts.abi.bep20_abi import BEP20_ABI
from artifacts.abi.events.transfer_event_abi import TRANSFER_EVENT_ABI



class ABI:
    mapping = {
        "bep20_abi": BEP20_ABI,
        "transfer_event_abi": TRANSFER_EVENT_ABI
    }

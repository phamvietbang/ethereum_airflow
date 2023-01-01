class Transaction:
    type = 'type'
    hash = 'hash'
    nonce = 'nonce'
    block_hash = 'block_hash'
    block_number = 'block_number'
    block_timestamp = 'block_timestamp'
    transaction_index = 'transaction_index'
    from_address = 'from_address'
    to_address = 'to_address'
    value = 'value'
    gas = 'gas'
    gas_price = 'gas_price'
    input = 'input'
    max_fee_per_gas = 'max_fee_per_gas'
    max_priority_fee_per_gas = 'max_priority_fee_per_gas'
    transaction_type = 'transaction_type'


class Log:
    log_index = "log_index"
    transaction_hash = "transaction_hash"
    transaction_index = "transaction_index"
    block_hash = "block_hash"
    block_number = "block_number"
    address = "address"
    data = "data"
    topics = "topics"


class Trace:
    log_index = "log_index"
    transaction_hash = "transaction_hash"
    transaction_index = "transaction_index"
    block_hash = "block_hash"
    block_number = "block_number"
    address = "address"
    data = "data"
    topics = "topics"

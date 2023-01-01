from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from tasks.export_block_transactions import export_blocks_transactions
from tasks.export_receipt_logs import export_receipt_logs
from tasks.export_events import export_events
from blockchainetl.streaming.exporter.s3_streaming_exporter import S3StreamingExporter
from web3 import Web3
from blockchainetl.streaming.streamer import *

item_exporter = S3StreamingExporter(
    access_key='AKIAXS2SFBSOTZFERP5O',
    secret_key='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',
    bucket='bangbich123',
    aws_region='us-east-1'
)

params = {
    'start_block': None,
    'end_block': None,
    'provider_uri': "https://rpc.onuschain.io/",
    'output': item_exporter,
    'chain': 'onus',
    'block_batch_size': 1200,
    'last_synced_block_file': "onus_last_synced_block_file.txt"
}

default_args = {
    'owner': "bangpv",
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


def get_current_block_number(provider_id):
    w3 = Web3(Web3.HTTPProvider(provider_id))
    return w3.eth.blockNumber


def calculate_target_block(current_block, block_batch_size, end_block, last_synced_block, lag=10):
    target_block = current_block - lag
    target_block = min(target_block, last_synced_block + block_batch_size)
    target_block = min(target_block, end_block) if end_block is not None else target_block
    return target_block


with DAG(
        default_args=default_args,
        dag_id='Athena_03',
        start_date=datetime(2022, 12, 26),
        schedule_interval='*/1 * * * *',
        max_active_runs=1
) as dag:
    @task
    def get_start_end_block(start_block, end_block, block_batch_size, last_synced_block_file, provider_id, ti):
        if start_block is not None or not os.path.isfile(last_synced_block_file):
            init_last_synced_block_file((start_block or 0) - 1, last_synced_block_file)

        last_synced_block = read_last_synced_block(last_synced_block_file)
        blocks_to_sync = 0
        target_block = end_block
        while (end_block is None or last_synced_block < end_block) and blocks_to_sync:
            current_block = get_current_block_number(provider_id)
            target_block = calculate_target_block(current_block, block_batch_size, end_block, last_synced_block)
            blocks_to_sync = max(target_block - last_synced_block, 0)

            logging.info('Current block {}, target block {}, last synced block {}, blocks to sync {}'.format(
                current_block, target_block, last_synced_block, blocks_to_sync))
        ti.xcom_push(key='start_batch_block', value=last_synced_block + 1)
        ti.xcom_push(key='end_batch_block', value=target_block)
        return last_synced_block + 1, target_block


    @task
    def get_transactions(ti, provider_uri, output, chain):
        start_block = ti.xcom_pull(task_ids='Get_start_end_block', key='start_batch_block')
        end_block = ti.xcom_pull(task_ids='Get_start_end_block', key='end_batch_block')
        transactions = export_blocks_transactions(
            start_block,
            end_block,
            provider_uri,
            output,
            chain,
        )
        hashes = []
        for transaction in transactions:
            hashes.append(transaction['hash'])

        ti.xcom_push(key='hash', value=hashes)
        return hashes


    @task
    def get_logs(ti, provider_uri, output, chain):
        transaction_hashes = ti.xcom_pull(task_ids='Get_transactions', key='hash')
        logs = export_receipt_logs(
            provider_uri=provider_uri,
            output=output,
            chain=chain,
            transaction_hashes=transaction_hashes,
        )
        ti.xcom_push(key='logs', value=logs)
        return logs


    @task
    def get_events(ti, output, chain):
        logs = ti.xcom_pull(task_ids='Get_logs', key='logs')
        export_events(output=output, logs=logs, chain=chain)


    @task
    def write_last_updated_block(ti, file):
        block = ti.xcom_pull(task_ids='Get_start_end_block', key='end_batch_block')
        write_last_synced_block(file, block)


    task1 = PythonOperator(
        task_id='Get_start_end_block',
        python_callable=get_start_end_block,
        op_kwargs={
            'start_block': params.get('start_block'),
            'end_block': params.get('end_block'),
            'block_batch_size': params.get('block_batch_size'),
            'last_synced_block_file': params.get('last_synced_block_file'),
            'provider_uri': params.get('provider_uri'),
        }
    )
    task2 = PythonOperator(
        task_id='Get_transactions',
        python_callable=get_transactions,
        op_kwargs={
            'provider_uri': params.get('provider_uri'),
            'output': params.get('output'),
            'chain': params.get('chain'),
        }
    )
    task3 = PythonOperator(
        task_id='Get_logs',
        python_callable=get_logs,
        op_kwargs={
            'provider_uri': params.get('provider_uri'),
            'output': params.get('output'),
            'chain': params.get('chain'),
        }
    )
    task4 = PythonOperator(
        task_id='Get_events',
        python_callable=get_events,
        op_kwargs={
            'output': params.get('output'),
            'chain': params.get('chain'),
        }
    )

    task5 = PythonOperator(
        task_id='Save_latest_updated_block',
        python_callable=write_last_updated_block,
        op_kwargs={
            'file': params.get('last_synced_block_file')
        }
    )

    task1 >> task2 >> task3 >> task4 >> task5

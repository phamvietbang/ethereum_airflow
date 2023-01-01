import json
import time
import boto3


class AthenaS3StreamingExporter(object):
    def __init__(self, access_key, secret_key, bucket, database, aws_region=None):
        self.database = database
        self.bucket = bucket
        if aws_region:
            self.session = boto3.session.Session(
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name=aws_region
            )
        else:
            self.session = boto3.session.Session(
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name=aws_region
            )
        self.s3_client = self.session.client('s3')
        self.athena_client = self.session.client('athena')
        self.config = {'OutputLocation': f's3://{self.bucket}/config'}

    def create_database(self):
        self.athena_client.start_query_execution(
            QueryString=f'create database if not exists {self.database}',
            ResultConfiguration=self.config)

    def create_event_table(self):
        query = f'''
        CREATE EXTERNAL TABLE IF NOT EXISTS `onus`.`events` (
            `event_type` string,
                `transaction_hash` string,
              `block_number` bigint,
              `log_index` bigint,
              `token_address` string,
              `from_address` string,
              `to_address` string,
              `value` double
            )
            ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
            WITH SERDEPROPERTIES (
              'ignore.malformed.json' = 'FALSE',
              'dots.in.keys' = 'FALSE',
              'case.insensitive' = 'TRUE',
              'mapping' = 'TRUE'
            )
            STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
            LOCATION 's3://{self.bucket}/{self.database}/events/'
            TBLPROPERTIES ('classification' = 'json');
            '''
        response = self.athena_client.start_query_execution(QueryString=query,
                                                            ResultConfiguration=self.config)
        return response["QueryExecutionId"]

    def create_erc20_token_table(self):
        query = f'''
            CREATE EXTERNAL TABLE IF NOT EXISTS `onus`.`tokens` (
              `address` string,
              `supply` string,
              `decimals` int
            )
            ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
            WITH SERDEPROPERTIES (
              'ignore.malformed.json' = 'FALSE',
              'dots.in.keys' = 'FALSE',
              'case.insensitive' = 'TRUE',
              'mapping' = 'TRUE'
            )
            STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
            LOCATION 's3://{self.bucket}/{self.database}/tokens/'
            TBLPROPERTIES ('classification' = 'json');
        '''
        response = self.athena_client.start_query_execution(QueryString=query,
                                                            ResultConfiguration=self.config)
        return response["QueryExecutionId"]

    def create_lp_token_table(self):
        query = f'''
        CREATE EXTERNAL TABLE IF NOT EXISTS `onus`.`lp_tokens` (
          `lp_token` string,
          `token_a` string,
          `token_b` string
        )
        ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
        WITH SERDEPROPERTIES (
          'ignore.malformed.json' = 'FALSE',
          'dots.in.keys' = 'FALSE',
          'case.insensitive' = 'TRUE',
          'mapping' = 'TRUE'
        )
        STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
        LOCATION 's3://{self.bucket}/{self.database}/lp_tokens/'
        TBLPROPERTIES ('classification' = 'json');
        '''
        response = self.athena_client.start_query_execution(QueryString=query,
                                                            ResultConfiguration=self.config)
        return response["QueryExecutionId"]

    def create_lp_token_holder_table(self):
        query = f'''
        CREATE EXTERNAL TABLE IF NOT EXISTS `onus`.`holders` (
          `lp_token` string,
          `address` string
        )
        ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
        WITH SERDEPROPERTIES (
          'ignore.malformed.json' = 'FALSE',
          'dots.in.keys' = 'FALSE',
          'case.insensitive' = 'TRUE',
          'mapping' = 'TRUE'
        )
        STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
        LOCATION 's3://{self.bucket}/{self.database}/holders/'
        TBLPROPERTIES ('classification' = 'json');
        '''
        response = self.athena_client.start_query_execution(QueryString=query,
                                                            ResultConfiguration=self.config)
        return response["QueryExecutionId"]

    def create_transaction_table(self):
        query = f'''
        CREATE EXTERNAL TABLE IF NOT EXISTS `onus`.`transactions` (
          `hash` string,
          `nonce` bigint,
          `block_hash` string,
          `block_number` bigint,
          `block_timestamp` bigint,
          `transaction_index` bigint,
          `from_address` string,
          `to_address` string,
          `value` string,
          `gas` string,
          `gas_price` string,
          `input` string
        )
        ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
        WITH SERDEPROPERTIES (
          'ignore.malformed.json' = 'FALSE',
          'dots.in.keys' = 'FALSE',
          'case.insensitive' = 'TRUE',
          'mapping' = 'TRUE'
        )
        STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
        LOCATION 's3://{self.bucket}/{self.database}/transactions/'
        TBLPROPERTIES ('classification' = 'json');
        '''
        response = self.athena_client.start_query_execution(QueryString=query,
                                                            ResultConfiguration=self.config)
        return response["QueryExecutionId"]

    def create_log_table(self):
        query = f'''
            CREATE EXTERNAL TABLE IF NOT EXISTS `onus`.`logs` (
              `address` string,
              `log_index` bigint,
              `data` string,
              `block_number` bigint,
              `transaction_index` bigint,
              `block_hash` string,
              `topics` array < string >,
              `transaction_hash` string
            )
            ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
            WITH SERDEPROPERTIES (
              'ignore.malformed.json' = 'FALSE',
              'dots.in.keys' = 'FALSE',
              'case.insensitive' = 'TRUE',
              'mapping' = 'TRUE'
            )
            STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
            LOCATION 's3://{self.bucket}/{self.database}/logs/'
            TBLPROPERTIES ('classification' = 'json');
            '''
        response = self.athena_client.start_query_execution(QueryString=query,
                                                            ResultConfiguration=self.config)
        return response["QueryExecutionId"]


    def export_holders(self):
        query = f'''
                create table holders
                with(
                    format = 'TEXTFILE',
                    field_delimiter = ',', 
                    external_location = 's3://{self.bucket}/{self.database}/holders', 
                    bucketed_by = ARRAY['address', 'token', 'balance'], 
                    bucket_count = 1)
                as(
                    select address, token, sum(value) as balance
                    from (
                    -- debits
                    select to_address as address, token_address as token, value as value
                    from onus.events
                    union all
                    -- credits
                    select from_address as address, token_address as token, -value as value
                    from onus.events
                    )
                    group by address, token
                    order by balance desc
                )'''
        response = self.athena_client.start_query_execution(QueryString=query,
                                                            ResultConfiguration=self.config)

        return response["QueryExecutionId"]

    def has_query_succeeded(self, execution_id):
        state = "RUNNING"
        max_execution = 5

        while max_execution > 0 and state in ["RUNNING", "QUEUED"]:
            max_execution -= 1
            response = self.athena_client.get_query_execution(QueryExecutionId=execution_id)
            if (
                    "QueryExecution" in response
                    and "Status" in response["QueryExecution"]
                    and "State" in response["QueryExecution"]["Status"]
            ):
                state = response["QueryExecution"]["Status"]["State"]
                if state == "SUCCEEDED":
                    return True

            time.sleep(30)

        return False

    def close(self):
        pass

    def open(self):
        pass


if __name__ == "__main__":
    # s3@AKIAXS2SFBSOTZFERP5O@GNHZ5JvbSoz5L8gjEvEbC0BRfk/XzuE7aHYLuycs@bangbich123@us-east-1
    job = AthenaS3StreamingExporter("AKIAXS2SFBSOTZFERP5O", "GNHZ5JvbSoz5L8gjEvEbC0BRfk/XzuE7aHYLuycs", "bangbich123",
                                    "onus", "us-east-1")
    m = job.get_log_by_block_number_and_hash("onus", 0, 5000000,
                                             "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
    while not job.has_query_succeeded(m):
        break

    data = job.get_log_data(m)

    print(data)

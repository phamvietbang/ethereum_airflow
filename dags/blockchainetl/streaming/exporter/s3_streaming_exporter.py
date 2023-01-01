import json
import time
import logging
import boto3

_LOGGER = logging.getLogger(__name__)


class S3StreamingExporter(object):
    def __init__(self, access_key, secret_key, bucket, aws_region=None):
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

    def export_item(self, chain, folder, data_string, key):
        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=f'{chain}/{folder}/{key}',
            Body=data_string
        )

    def export_items(self, chain, folder, data):
        for data_dict in data:
            data_string = json.dumps(data_dict, default=str)
            if folder in ["transactions", 'receipts']:
                self.export_item(chain, folder, data_string, data_dict['hash'])
            elif folder == "lp_tokens":
                self.export_item(chain, folder, data_string, data_dict["lp_token"])
            else:
                self.export_item(chain, folder, data_string, f'{data_dict["block_number"]}_{data_dict["log_index"]}')

    def export_lp_tokens(self, chain, folder, data):
        for data_dict in data:
            data_string = json.dumps(data_dict, default=str)
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=f'{chain}/{folder}/{data_dict["lp_token"]}',
                Body=data_string
            )

    def export_holders(self, chain, folder, data):
        for data_dict in data:
            data_string = json.dumps(data_dict, default=str)
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=f'{chain}/{folder}/{data_dict["lp_token"]}_{data_dict["address"]}',
                Body=data_string
            )

    def export_tokens(self, chain, folder, data):
        for data_dict in data:
            data_string = json.dumps(data_dict, default=str)
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=f'{chain}/{folder}/{data_dict["address"]}',
                Body=data_string
            )

    def get_collector(self, chain, stream_id):
        pass

    def update_collector(self, chain, stream_id):
        pass

    def update_latest_updated_at(self, chain, stream_id, file):
        pass

    def close(self):
        pass

    def open(self):
        pass
# s3@AKIAXS2SFBSOTZFERP5O@GNHZ5JvbSoz5L8gjEvEbC0BRfk/XzuE7aHYLuycs@bangbich123@us-east-1

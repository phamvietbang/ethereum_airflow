from blockchainetl.streaming.streamer_adapter_stub import StreamerAdapterStub
from configs.blockchain_etl_config import BlockchainEtlConfig

class RoundTimeStreamer(Streamer):
    def __init__(
            self,
            blockchain_streamer_adapter=StreamerAdapterStub(),
            last_synced_block_file='last_synced_block.txt',
            lag=0,
            start_block=None,
            end_block=None,
            period_seconds=10,
            block_batch_size=10,
            retry_errors=True,
            pid_file=None,
            stream_id=BlockchainEtlConfig.STREAM_ID,
            output=BlockchainEtlConfig.OUTPUT,
            db_prefix="",
    ):
        super().__init__(
            blockchain_streamer_adapter=blockchain_streamer_adapter,
            last_synced_block_file=last_synced_block_file,
            lag=lag,
            start_block=start_block,
            end_block=end_block,
            period_seconds=period_seconds,
            block_batch_size=block_batch_size,
            retry_errors=retry_errors,
            pid_file=pid_file,
            stream_id=stream_id,
            output=output,
            db_prefix=db_prefix
        )

    def _do_stream(self):
        while True and (self.end_block is None or self.last_synced_block < self.end_block):
            synced_blocks = 0
            try:
                synced_blocks = self._sync_cycle()
                if synced_blocks <= 0:
                    logging.info('Nothing to sync. Sleeping for {} seconds...'.format(self.period_seconds))
                    time.sleep(self.period_seconds)
            except Exception as e:
                # https://stackoverflow.com/a/4992124/1580227
                logging.exception('An exception occurred while syncing block data.')
                if not self.retry_errors:
                    raise e

            if synced_blocks <= 0:
                time.sleep(10)
import logging
from pymongo import UpdateOne
import time
from pymongo import MongoClient

logger = logging.getLogger("MongodbStreamingExporter")


class MongodbStreamingExporter(object):
    """Manages connection to  database and makes async queries
    """

    def __init__(self, connection_url):
        self._conn = None
        self.mongo = MongoClient(connection_url)

    def get_collector(self, db, collector_id):
        return self.mongo[db]["config"].find_one({"_id": collector_id})

    def get_items(self, db, collection, _filter):
        return self.mongo[db][collection].find(_filter)

    def update_collector(self, db, collector):
        key = {'_id': collector['_id']}
        data = {"$set": collector}

        self.mongo[db]["config"].update_one(key, data, upsert=True)

    def update_latest_updated_at(self, db, collector_id, latest_updated_at):
        key = {'_id': collector_id}
        update = {"$set": {
            "last_updated_at_block_number": latest_updated_at
        }}
        self.mongo[db]['config'].update_one(key, update)

    def export_items(self, db, collection, operations_data):
        if not operations_data:
            logger.debug(f"Error: Don't have any data to write")
            return
        start = time.time()
        bulk_operations = [UpdateOne({'_id': data['_id']}, {"$set": data}, upsert=True) for data in operations_data]
        logger.info(f"Updating into collection {collection} ........")
        try:
            self.mongo[db][collection].bulk_write(bulk_operations)
        except Exception as bwe:
            logger.error(f"Error: {bwe}")
        end = time.time()
        logger.info(f"Success write data to database take {end - start}s")

    def close(self):
        pass

    def open(self):
        pass

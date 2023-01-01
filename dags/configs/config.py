import os


class MongoDBConfig:
    NAME = os.environ.get("MONGO_USERNAME") or "just_for_dev"
    PASSWORD = os.environ.get("MONGO_PASSWORD") or "password_for_dev"
    HOST = os.environ.get("MONGO_HOST") or "localhost"
    PORT = os.environ.get("MONGO_PORT") or "27027"


class Provider:
    BSC_PUBLIC_RPC_NODE: str = "https://bsc-dataseed1.binance.org/"
    ANKR_PUBLIC_ETH_RPC_NODE: str = "https://rpc.ankr.com/eth"

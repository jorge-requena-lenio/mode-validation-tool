import os
from dotenv import load_dotenv

load_dotenv()


def getenv(key: str) -> str:
    value = os.getenv(key)
    assert value is not None, f"Environment variable {key} not found"
    return value


config = {
    "mode": {
        "host": "https://app.mode.com",
        "account": getenv("MODE_ACCOUNT"),
        "username": getenv("MODE_KEY"),
        "password": getenv("MODE_SECRET"),
    },
    "aws": {
        "bucket_name": getenv("BUCKET_NAME"),
        "access_key_id": getenv("AWS_ACCESS_KEY_ID"),
        "secret_access_key": getenv("AWS_SECRET_ACCESS_KEY"),
    },
}

import os
import logging
from google.cloud.logging import Client, Resource
from google.cloud.logging.handlers import CloudLoggingHandler


def setup_logger():
    """ルートロガーに CloudLoggingの設定をする"""

    logging_client = Client()
    resource = Resource(
        type="cloud_function",
        labels={
            "function_name": os.environ.get("_FUNCTION_NAME"),
            "project_id": os.environ.get("_PROJECT_ID"),
            "region": os.environ.get("_REGION"),
        },
    )
    handler = CloudLoggingHandler(logging_client, resource=resource)
    logging.getLogger().setLevel(logging.INFO)
    setup_logging(handler)

    return

import logging
from google.cloud.logging import Client
from google.cloud.logging.handlers import CloudLoggingHandler

logging_client = Client()
handler = CloudLoggingHandler(logging_client)
cloud_logger = logging.getLogger()
cloud_logger.setLevel(logging.INFO)
cloud_logger.addHandler(handler)

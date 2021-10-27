import base64

from logging import getLogger
from logging import DEBUG
from logging import StreamHandler
from logging import Formatter

logger = getLogger(__name__)
logger.setLevel(DEBUG)
handler = StreamHandler()
handler.setLevel(DEBUG)
logger.addHandler(handler)


def main(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    pubsub_message = base64.b64decode(event["data"]).decode("utf-8")
    # ただのトリガーなのでpubsumメッセージ内容は無視

    # 全気象台分リクエスト実行しcsv出力

    # 出力したCSVファイルをBigQueryへinsert

    logger.info("finish tenmado-load")

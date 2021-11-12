import base64
import logging

from services import weatherforcastservice

from utils.logger import setup_logger


def main(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    pubsub_message = base64.b64decode(event["data"]).decode("utf-8")
    # ただのトリガーなのでpubsumメッセージ内容は無視

    # loggerの設定
    setup_logger()
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    try:
        # 全気象台分リクエスト実行しローカルにcsv出力→ GCSアップロード
        weatherforcastservice.request_weather_forecast()

        # 出力したCSVファイルをBigQueryへinsert
        weatherforcastservice.gcsweatherforecastfiles_to_bqtable()

        logger.info("[completed] tenmado-load")

    except Exception as e:

        logger.exception("tenmado-load error")

    finally:
        # ローカルcsvを削除
        weatherforcastservice.delete_localweatherforecastfiles()

        # GCSのcsvを削除
        weatherforcastservice.delete_insertedgcsweatherforecastfiles()

    return

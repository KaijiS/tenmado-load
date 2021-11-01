import base64

from services import weatherforcastservice

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

    try:
        # 全気象台分リクエスト実行しローカルにcsv出力
        weatherforcastservice.request_weather_forecast()

        # 出力したcsvをGCSへアップロード
        weatherforcastservice.upload_weatherforecastfiles_to_gcs()

        # 出力したCSVファイルをBigQueryへinsert
        weatherforcastservice.gcsweatherforecastfiles_to_bqtable()

        logger.info("finish tenmado-load")

    except Exception as e:
        # TODO GCSのファイルをエラーフォルダにコピーするか相談

        logger.exception("tenmado-load error")

    finally:
        # ローカルcsvを削除
        weatherforcastservice.delete_localweatherforecastfiles()

        # GCSのcsvを削除
        weatherforcastservice.delete_insertedgcsweatherforecastfiles()

    return

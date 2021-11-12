import datetime
import logging

import pandas as pd

from modules.weatherforcast import WeatherForecast
from utils import gcs
from utils import bq
from utils import files
from utils import jinja2
from utils import decorator

# loggerの設定
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def fetch_meteorological_observatory_codes(project_id: str):
    """気象庁コード一覧取得
    return
        気象庁コードの一覧リスト
    """
    query_base: str = files.read_file("sqls/fetch_meteorological_observatory_codes.sql")
    query = jinja2.embed_to_query(
        query_base=query_base, params={"project_id": project_id}
    )

    results = bq.exe_query(query)
    meteorological_observatory_codes = [
        row.meteorological_observatory_code for row in results
    ]

    return meteorological_observatory_codes


@decorator.set_config
def request_weather_forecast(config):
    """予報をリクエストしcsvファイル出力しGCSへアップロード
    Args
        config: 設定値
    """

    # 気象庁コード一覧取得
    meteorological_observatory_codes = fetch_meteorological_observatory_codes(
        project_id=config["project_id"]
    )

    # 各DFを結合するためのリストを準備
    fewdays_weather_dfs: list[pd.DataFrame] = []
    tomorrow_pops_dfs: list[pd.DataFrame] = []
    tomorrow_temps_dfs: list[pd.DataFrame] = []
    week_weather_dfs: list[pd.DataFrame] = []
    week_temps_dfs: list[pd.DataFrame] = []
    past_tempavg_dfs: list[pd.DataFrame] = []
    past_precopitationavg_dfs: list[pd.DataFrame] = []

    # それぞれの気象庁コードに対してリクエストし、予報を集約したDataFrameを得る
    for meteorological_observatory_code in meteorological_observatory_codes:

        # コンストラクタでリクエストとDataFrameへの格納をしている
        weather_forcast = WeatherForecast(meteorological_observatory_code)

        # 各DataFrameをリストに追加(後で結合)
        fewdays_weather_dfs.append(weather_forcast.fewdays_weather_df)
        tomorrow_pops_dfs.append(weather_forcast.tomorrow_pops_df)
        tomorrow_temps_dfs.append(weather_forcast.tomorrow_temps_df)
        week_weather_dfs.append(weather_forcast.week_weather_df)
        week_temps_dfs.append(weather_forcast.week_temps_df)
        past_tempavg_dfs.append(weather_forcast.past_tempavg_df)
        past_precopitationavg_dfs.append(weather_forcast.past_precopitationavg_df)

    # DataFrame結合
    fewdays_weather_df = pd.concat(fewdays_weather_dfs)
    tomorrow_pops_df = pd.concat(tomorrow_pops_dfs)
    tomorrow_temps_df = pd.concat(tomorrow_temps_dfs)
    week_weather_df = pd.concat(week_weather_dfs)
    week_temps_df = pd.concat(week_temps_dfs)
    past_tempavg_df = pd.concat(past_tempavg_dfs)
    past_precopitationavg_df = pd.concat(past_precopitationavg_dfs)

    # ファイル出力し GCSへアップロード
    files.to_csvfile(
        df=fewdays_weather_df,
        filename=config["import_data"]["fewdays_weather"]["filename"],
        local_dir=config["tmp_file_dir"],
        bucket_name=config["bucket_name"],
        gcs_filename_prefix=config["gcs_import_dir"],
        index=False,
    )
    files.to_csvfile(
        df=tomorrow_pops_df,
        filename=config["import_data"]["tomorrow_pops"]["filename"],
        local_dir=config["tmp_file_dir"],
        bucket_name=config["bucket_name"],
        gcs_filename_prefix=config["gcs_import_dir"],
        index=False,
    )
    files.to_csvfile(
        df=tomorrow_temps_df,
        filename=config["import_data"]["tomorrow_temps"]["filename"],
        local_dir=config["tmp_file_dir"],
        bucket_name=config["bucket_name"],
        gcs_filename_prefix=config["gcs_import_dir"],
        index=False,
    )
    files.to_csvfile(
        df=week_weather_df,
        filename=config["import_data"]["week_weather"]["filename"],
        local_dir=config["tmp_file_dir"],
        bucket_name=config["bucket_name"],
        gcs_filename_prefix=config["gcs_import_dir"],
        index=False,
    )
    files.to_csvfile(
        df=week_temps_df,
        filename=config["import_data"]["week_temps"]["filename"],
        local_dir=config["tmp_file_dir"],
        bucket_name=config["bucket_name"],
        gcs_filename_prefix=config["gcs_import_dir"],
        index=False,
    )
    files.to_csvfile(
        df=past_tempavg_df,
        filename=config["import_data"]["past_tempavg"]["filename"],
        local_dir=config["tmp_file_dir"],
        bucket_name=config["bucket_name"],
        gcs_filename_prefix=config["gcs_import_dir"],
        index=False,
    )
    files.to_csvfile(
        df=past_precopitationavg_df,
        filename=config["import_data"]["past_precopitationavg"]["filename"],
        local_dir=config["tmp_file_dir"],
        bucket_name=config["bucket_name"],
        gcs_filename_prefix=config["gcs_import_dir"],
        index=False,
    )

    return


@decorator.set_config
def gcsweatherforecastfiles_to_bqtable(config):
    """GCS上に保存した予報CSVファイルをBQのテーブルへinsert
    Args
        config: 設定値
    """

    # エラーディレクトリ用タイムスタンプを準備
    now = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=9), "JST"))
    now_str = now.strftime("%Y%m%d%H%M%S")

    for data in config["import_data"].values():

        try:
            bq.file_to_table(
                project_id=config["project_id"],
                dataset_name=config["import_datasetname"],
                table_name=data["import_table_name"],
                table_schema_path=data["table_schema_path"],
                source_file_uri=f"gs://{config['bucket_name']}/{config['gcs_import_dir']}/{data['filename']}",
                replace=False,
                partition_field=data["partition_field"],
                skip_leading_rows=data["skip_leading_rows"],
            )
        except:
            gcs.copy_blob(
                bucket_name=config["bucket_name"],
                blob_name=f"{config['gcs_import_dir']}/{data['filename']}",
                destination_bucket_name=config["bucket_name"],
                destination_blob_name=f"{config['gcs_error_dir']}/{now_str}/{data['filename']}",
            )
            logger.error(f"Import Error: {data['filename']} to BigQuery Table")

    return


@decorator.set_config
def delete_localweatherforecastfiles(config):
    """リクエスト後ローカルに保存したの予報CSVファイルを削除
    Args
        config: 設定値
    """
    for data in config["import_data"].values():
        files.delete_file(
            filepath=f"{config['tmp_file_dir']}/{data['filename']}",
        )
    return


@decorator.set_config
def delete_insertedgcsweatherforecastfiles(config):
    """BQへinsertされたGCS上の予報CSVファイルを削除
    Args
        config: 設定値
    """
    for data in config["import_data"].values():
        gcs.delete_blob(
            bucket_name=config["bucket_name"],
            blob_name=f"{config['gcs_import_dir']}/{data['filename']}",
        )
    return

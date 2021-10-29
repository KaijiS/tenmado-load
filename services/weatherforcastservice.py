from modules.weatherforcast import WeatherForecast
from utils import gcs
from utils import files
from utils import jinja2
from utils import decorator

from logging import getLogger
from logging import DEBUG
from logging import StreamHandler
from logging import Formatter

logger = getLogger(__name__)
logger.setLevel(DEBUG)
handler = StreamHandler()
handler.setLevel(DEBUG)
logger.addHandler(handler)


def fetch_meteorological_observatory_codes(project_id: str):
    """気象庁コード一覧取得
    return
        気象庁コードの一覧リスト
    """
    query_base: str = files.read_file("sqls/fetch_meteorological_observatory_codes.sql")
    query = jinja2.embed_to_query(
        query_base=query_base, params={"project_id": project_id}
    )

    result = exe_query(query)
    meteorological_observatory_codes = [
        row.meteorological_observatory_code for row in results
    ]

    return meteorological_observatory_codes


@decorator.set_config
def request_weather_forecast(config):
    """予報をリクエストしcsvファイル出力
    Args
        config: 設定値
    """

    # 気象庁コード一覧取得
    meteorological_observatory_codes = fetch_meteorological_observatory_codes(
        project_id=config.project_id
    )

    # 各DFを結合するためのリストを準備
    fewdays_weather_dfs: list[pd.DataFrame] = []
    tomorrow_pops_dfs: list[pd.DataFrame] = []
    tomorrow_temps_df: list[pd.DataFrame] = []
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

    # ファイル出力
    fewdays_weather_df.to_csv(
        f"{config.tmp_file_dir}/{config.fewdays_weather_filename}"
    )
    tomorrow_pops_df.to_csv(f"{config.tmp_file_dir}/{config.tomorrow_pops_filename}")
    tomorrow_temps_df.to_csv(f"{config.tmp_file_dir}/{config.tomorrow_temps_filename}")
    week_weather_df.to_csv(f"{config.tmp_file_dir}/{config.week_weather_filename}")
    week_temps_df.to_csv(f"{config.tmp_file_dir}/{config.week_temps_filename}")
    past_tempavg_df.to_csv(f"{config.tmp_file_dir}/{config.past_tempavg_filename}")
    past_precopitationavg_df.to_csv(
        f"{config.tmp_file_dir}/{config.past_precopitationavg_filename}"
    )

    return


@decorator.set_config
def upload_weatherforecastfiles_to_gcs(config):
    """気象庁コード一覧取得
    Args
        config: 設定値
    """

    filenames = [
        config.fewdays_weather_filename,
        config.tomorrow_pops_filename,
        config.tomorrow_temps_filename,
        config.week_weather_filename,
        config.week_temps_filename,
        config.past_tempavg_filename,
        config.past_precopitationavg_filename,
    ]

    # GCSへ順にアップロード
    for filename in filenames:
        gcs.to_gcs(
            bucket_name=config.bucket_name,
            filepath=f"{config.gcs_import_dir}/{filename}",
            upload_path=f"{config.tmp_file_dir}/{filename}",
        )
    return

@decorator.set_config
def gcsweatherforecastfiles_to_bqtable(config):

    file_to_table(
        project_id=config.project_id,
        dataset_name=config.import_datasetname,
        table_name=table_name,
        table_schema_path=schemapath,
        source_file_uri: str,
        replace: bool = False,
        partition_field: str = None,
        skip_leading_rows: int = 1,
    )

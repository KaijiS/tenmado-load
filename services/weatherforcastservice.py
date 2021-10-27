from modules.weatherforcast import WeatherForecast
from utils import gcs
from utils import files
from utils import jinja2
import config

from logging import getLogger
from logging import DEBUG
from logging import StreamHandler
from logging import Formatter

logger = getLogger(__name__)
logger.setLevel(DEBUG)
handler = StreamHandler()
handler.setLevel(DEBUG)
logger.addHandler(handler)


def fetch_meteorological_observatory_codes():
    """気象庁コード一覧取得
    return
        気象庁コードの一覧リスト
    """
    query_base: str = files.read_file("sqls/fetch_meteorological_observatory_codes.sql")
    query = jinja2.embed_to_query(
        query_base=query_base, params={"project_id": config.PROJECT_ID}
    )

    result = exe_query(query)
    meteorological_observatory_codes = [
        row.meteorological_observatory_code for row in results
    ]

    return meteorological_observatory_codes


def aaa():

    # 気象庁コード一覧取得
    meteorological_observatory_codes = fetch_meteorological_observatory_codes()

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

    fewdays_weather_df = pd.concat(fewdays_weather_dfs)
    tomorrow_pops_df = pd.concat(tomorrow_pops_dfs)
    tomorrow_temps_df = pd.concat(tomorrow_temps_dfs)
    week_weather_df = pd.concat(week_weather_dfs)
    week_temps_df = pd.concat(week_temps_dfs)
    past_tempavg_df = pd.concat(past_tempavg_dfs)
    past_precopitationavg_df = pd.concat(past_precopitationavg_dfs)

    fewdays_weather_df.to_csv(".tmp_files/fewdays_weather.csv")
    tomorrow_pops_df.to_csv(".tmp_files/tomorrow_pops.csv")
    tomorrow_temps_df.to_csv(".tmp_files/tomorrow_temps.csv")
    week_weather_df.to_csv(".tmp_files/week_weather.csv")
    week_temps_df.to_csv(".tmp_files/week_tempsr.csv")
    past_tempavg_df.to_csv(".tmp_files/past_tempavg.csv")
    past_precopitationavg_df.to_csv(".tmp_files/past_precopitationavg.csv")

    # GCSへアップロード
    gcs.to_gcs(
        bucket_name="バケット名",
        filepath="GCSファイルパス",
        upload_path=".tmp_files/fewdays_weather.csv",
    )

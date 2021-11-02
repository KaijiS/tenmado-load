import requests
import json
import datetime
import numpy as np
import pandas as pd

from typing import Any

from logging import getLogger
from logging import DEBUG
from logging import StreamHandler
from logging import Formatter

logger = getLogger(__name__)
logger.setLevel(DEBUG)
handler = StreamHandler()
handler.setLevel(DEBUG)
logger.addHandler(handler)


class WeatherForecast:
    def __init__(self, area_code):

        """
        フィールド変数
        self.area_code: str
        self.get_datetime: datetime.datetime
        self.response_dict: dict[str, Any]
        self.fewdays_weather_df: pd.DataFrame
        self.tomorrow_pops_df: pd.DataFrame
        self.tomorrow_temps_df: pd.DataFrame
        self.week_weather_df: pd.DataFrame
        self.week_temps_df: pd.DataFrame
        self.past_tempavg_df: pd.DataFrame
        self.past_pricipitationavg_df: pd.DataFrame
        """

        self.area_code = area_code
        # 取得日
        self.get_datetime = datetime.datetime.now(
            datetime.timezone(datetime.timedelta(hours=9), "JST")
        ).strftime("%Y-%m-%d %H:%M:%S")

        # 予報APIを叩く
        url = f"https://www.jma.go.jp/bosai/forecast/data/forecast/{area_code}.json"
        response = requests.get(url)

        self.response_dict = json.loads(response.text)

        # レスポンス情報から各種予報データ取得
        self.__extract_forecast_from_response()

    def __extract_forecast_from_response(self):
        """
        レスポンス内容から各予報値を取得しそれぞれのDataFrameに格納する
        """

        # 明日明後日分の情報([0])を取得
        fewdays_forecast_response_dict = self.response_dict[0]
        # 気象情報レポート日時
        report_datetime: datetime.date = fewdays_forecast_response_dict[
            "reportDatetime"
        ]
        # 気象台名
        meteorological_observatory_name = fewdays_forecast_response_dict[
            "publishingOffice"
        ]
        # 明日明後日の予報情報を取得
        self.__extract_fewdays_weather(
            fewdays_weather_dict=fewdays_forecast_response_dict["timeSeries"][0],
            report_datetime=report_datetime,
            meteorological_observatory_name=meteorological_observatory_name,
        )
        # 明日の降水確率を取得
        self.__extract_tomorrow_pops(
            tomorrow_pops_dict=fewdays_forecast_response_dict["timeSeries"][1],
            report_datetime=report_datetime,
            meteorological_observatory_name=meteorological_observatory_name,
        )
        # 明日の気温を取得(代表都市)
        self.__extract_tomorrow_temps(
            tomorrow_temps_dict=fewdays_forecast_response_dict["timeSeries"][2],
            report_datetime=report_datetime,
            meteorological_observatory_name=meteorological_observatory_name,
        )

        # 1週間分の情報([1])を取得
        week_forecast_response_dict = self.response_dict[1]
        # 気象情報レポート日時
        report_datetime: datetime.date = week_forecast_response_dict["reportDatetime"]
        # 気象台名
        meteorological_observatory_name = week_forecast_response_dict[
            "publishingOffice"
        ]
        # 1週間分の天気情報を取得
        self.__extract_week_weather(
            week_weather_dict=week_forecast_response_dict["timeSeries"][0],
            report_datetime=report_datetime,
            meteorological_observatory_name=meteorological_observatory_name,
        )
        # 1週間分の気温を取得(代表都市)
        self.__extract_week_temps(
            week_temps_dict=week_forecast_response_dict["timeSeries"][1],
            report_datetime=report_datetime,
            meteorological_observatory_name=meteorological_observatory_name,
        )

        # 向こう1週間の平年気温
        self.__extract_past_tempavg(
            past_tempavg_dict=week_forecast_response_dict["tempAverage"],
            report_datetime=report_datetime,
            meteorological_observatory_name=meteorological_observatory_name,
        )

        # 向こう1週間の平年降水量
        self.__extract_past_precipitationavg(
            past_precipitationavg_dict=week_forecast_response_dict["precipAverage"],
            report_datetime=report_datetime,
            meteorological_observatory_name=meteorological_observatory_name,
        )

        return

    def __extract_fewdays_weather(
        self,
        fewdays_weather_dict: dict[str, Any],
        report_datetime: datetime.datetime,
        meteorological_observatory_name: str,
    ):
        """
        数日分(2日分?)の予報値を抽出しDataFrameに格納する
        Args:
            fewdays_weather_dict: dict[str, Any]: 該当予報値辞書
            report_datetime: datetime.datetime: 気象情報レポート日時
            meteorological_observatory_name: str: 気象台名
        """

        # 地方コード
        area_codes: list[str] = []
        # 地方名
        area_names: list[str] = []
        # 予報対象日
        forecast_target_dates: list[datetime.date] = []
        # 天気コード
        weather_codes: list[str] = []
        # 天気
        weathers: list[str] = []
        # 風
        winds: list[str] = []
        # 波
        waves: list[str] = []

        # 明日明後日の日時情報を準備
        datetimes = fewdays_weather_dict["timeDefines"][1:]
        # エリアごとの情報
        areas = fewdays_weather_dict["areas"]

        for area in areas:
            try:
                area_codes += [area["area"]["code"]] * len(datetimes)
                area_names += [area["area"]["name"]] * len(datetimes)
                forecast_target_dates += datetimes
                weather_codes += area["weatherCodes"][1:]
                weathers += area["weathers"][1:]
                winds += area["winds"][1:]
                waves += area["waves"][1:]
            except Exception as e:
                logger.exception(f"area is {area}")

        fewdays_weather_df = pd.DataFrame(
            {
                "area_code": area_codes,
                "area_name": area_names,
                "forecast_target_date": forecast_target_dates,
                "weather_code": weather_codes,
                "weather": weathers,
                "winds": winds,
                "waves": waves,
            }
        )

        # 取得日時
        fewdays_weather_df["get_datetime"] = self.get_datetime
        # 気象情報レポート日時
        fewdays_weather_df["report_datetime"] = report_datetime
        # 気象台名
        fewdays_weather_df[
            "meteorological_observatory_name"
        ] = meteorological_observatory_name

        # 列並び替え
        self.fewdays_weather_df = fewdays_weather_df[
            [
                "get_datetime",
                "report_datetime",
                "meteorological_observatory_name",
                "area_code",
                "area_name",
                "forecast_target_date",
                "weather_code",
                "weather",
                "winds",
                "waves",
            ]
        ]

    def __extract_tomorrow_pops(
        self,
        tomorrow_pops_dict: dict[str, Any],
        report_datetime: datetime.datetime,
        meteorological_observatory_name: str,
    ):
        """
        明日分の降水確率予報値を抽出しDataFrameに格納する
        Args:
            fewdays_weather_dict: dict[str, Any]: 該当予報値辞書
            report_datetime: datetime.datetime: 気象情報レポート日時
            meteorological_observatory_name: str: 気象台名
        """

        # 地方コード
        area_codes: list[str] = []
        # 地方名
        area_names: list[str] = []
        # 予報対象日
        forecast_target_dates: list[datetime.date] = []
        # 降水確率0-6 (pop = probability of precipitation の略)
        pops0006: list[int] = []
        # 降水確率6-12
        pops0612: list[int] = []
        # 降水確率12-18
        pops1218: list[int] = []
        # 降水確率18-24
        pops1824: list[int] = []

        # 降水確率予報日
        datetimes = tomorrow_pops_dict["timeDefines"][1]

        # エリアごとの降水確率
        areas = tomorrow_pops_dict["areas"]

        for area in areas:
            try:
                area_codes += [area["area"]["code"]]
                area_names += [area["area"]["name"]]
                forecast_target_dates += [datetimes]
                pops0006 += [area["pops"][1]]
                pops0612 += [area["pops"][2]]
                pops1218 += [area["pops"][3]]
                pops1824 += [area["pops"][4]]
            except Exception as e:
                logger.exception(f"area is {area}")

        tomorrow_pops_df = pd.DataFrame(
            {
                "area_code": area_codes,
                "area_name": area_names,
                "forecast_target_date": forecast_target_dates,
                "pops0006": pops0006,
                "pops0612": pops0612,
                "pops1218": pops1218,
                "pops1824": pops1824,
            }
        )

        # 取得日時
        tomorrow_pops_df["get_datetime"] = self.get_datetime
        # 気象情報レポート日時
        tomorrow_pops_df["report_datetime"] = report_datetime
        # 気象台名
        tomorrow_pops_df[
            "meteorological_observatory_name"
        ] = meteorological_observatory_name

        # 列並び替え
        self.tomorrow_pops_df = tomorrow_pops_df[
            [
                "get_datetime",
                "report_datetime",
                "meteorological_observatory_name",
                "area_code",
                "area_name",
                "forecast_target_date",
                "pops0006",
                "pops0612",
                "pops1218",
                "pops1824",
            ]
        ]

    def __extract_tomorrow_temps(
        self,
        tomorrow_temps_dict: dict[str, Any],
        report_datetime: datetime.datetime,
        meteorological_observatory_name: str,
    ):
        """
        明日分の気温予報値を抽出しDataFrameに格納する
        Args:
            fewdays_weather_dict: dict[str, Any]: 該当予報値辞書
            report_datetime: datetime.datetime: 気象情報レポート日時
            meteorological_observatory_name: str: 気象台名
        """

        # 代表都市コード
        city_codes: list[str] = []
        # 代表都市名
        city_names: list[str] = []
        # 予報対象日
        forecast_target_dates: list[datetime.date] = []
        # 最高気温
        lowest_temperatures: list[float] = []
        # 最低気温
        highest_temperatures: list[float] = []

        # 気温予報日
        datetimes = tomorrow_temps_dict["timeDefines"][0]
        # エリアごとの気温
        areas = tomorrow_temps_dict["areas"]

        for area in areas:
            try:
                city_codes += [area["area"]["code"]]
                city_names += [area["area"]["name"]]
                forecast_target_dates += [datetimes]
                lowest_temperatures += [area["temps"][0]]
                highest_temperatures += [area["temps"][1]]
            except Exception as e:
                logger.exception(f"area is {area}")

        tomorrow_temps_df = pd.DataFrame(
            {
                "city_code": city_codes,
                "city_name": city_names,
                "forecast_target_date": forecast_target_dates,
                "lowest_temperature": lowest_temperatures,
                "highest_temperature": highest_temperatures,
            }
        )

        # 取得日時
        tomorrow_temps_df["get_datetime"] = self.get_datetime
        # 気象情報レポート日時
        tomorrow_temps_df["report_datetime"] = report_datetime
        # 気象台名
        tomorrow_temps_df[
            "meteorological_observatory_name"
        ] = meteorological_observatory_name

        self.tomorrow_temps_df = tomorrow_temps_df[
            [
                "get_datetime",
                "report_datetime",
                "meteorological_observatory_name",
                "city_code",
                "city_name",
                "forecast_target_date",
                "lowest_temperature",
                "highest_temperature",
            ]
        ]

    def __extract_week_weather(
        self,
        week_weather_dict: dict[str, Any],
        report_datetime: datetime.datetime,
        meteorological_observatory_name: str,
    ):
        """
        1週間分の気象予報値を抽出しDataFrameに格納する
        Args:
            fewdays_weather_dict: dict[str, Any]: 該当予報値辞書
            report_datetime: datetime.datetime: 気象情報レポート日時
            meteorological_observatory_name: str: 気象台名
        """

        # 変数宣言
        area_codes: list[str] = []
        area_names: list[str] = []
        forecast_target_dates: list[datetime.date] = []
        weather_codes: list[str] = []
        pops: list[int] = []
        reliabilities: list[str] = []

        # 1週間の日時情報を準備
        datetimes = week_weather_dict["timeDefines"]
        # エリアごとの情報
        areas = week_weather_dict["areas"]

        for area in areas:
            try:
                area_codes += [area["area"]["code"]] * len(datetimes)
                area_names += [area["area"]["name"]] * len(datetimes)
                forecast_target_dates += datetimes
                # 天気コード
                weather_codes += area["weatherCodes"]
                # 降水確率
                pops += [i if i != "" else np.nan for i in area["pops"]]
                # 信頼度
                reliabilities += [
                    i if i != "" else np.nan for i in area["reliabilities"]
                ]
            except Exception as e:
                logger.exception(f"area is {area}")

        week_weather_df = pd.DataFrame(
            {
                "area_code": area_codes,
                "area_name": area_names,
                "forecast_target_date": forecast_target_dates,
                "weather_code": weather_codes,
                "pop": pops,
                "reliability": reliabilities,
            }
        )

        # 取得日時
        week_weather_df["get_datetime"] = self.get_datetime
        # 気象情報レポート日時
        week_weather_df["report_datetime"] = report_datetime
        # 気象台名
        week_weather_df[
            "meteorological_observatory_name"
        ] = meteorological_observatory_name

        self.week_weather_df = week_weather_df[
            [
                "get_datetime",
                "report_datetime",
                "meteorological_observatory_name",
                "area_code",
                "area_name",
                "forecast_target_date",
                "weather_code",
                "pop",
                "reliability",
            ]
        ]

    def __extract_week_temps(
        self,
        week_temps_dict: dict[str, Any],
        report_datetime: datetime.datetime,
        meteorological_observatory_name: str,
    ):
        """
        1週間分の気温予報値を抽出しDataFrameに格納する
        Args:
            fewdays_weather_dict: dict[str, Any]: 該当予報値辞書
            report_datetime: datetime.datetime: 気象情報レポート日時
            meteorological_observatory_name: str: 気象台名
        """

        city_codes: list[str] = []
        city_names: list[str] = []
        forecast_target_dates: list[datetime.date] = []
        lowest_temperatures: list[float] = []
        lowest_temperature_uppers: list[float] = []
        lowest_temperature_lowers: list[float] = []
        highest_temperatures: list[float] = []
        highest_temperature_uppers: list[float] = []
        highest_temperature_lowers: list[float] = []

        # 1週間の日時情報を準備
        datetimes = week_temps_dict["timeDefines"]
        # エリアごとの情報
        cities = week_temps_dict["areas"]

        for city in cities:

            try:
                city_codes += [city["area"]["code"]] * len(datetimes)
                city_names += [city["area"]["name"]] * len(datetimes)
                forecast_target_dates += datetimes
                # 最低気温
                lowest_temperatures += [
                    i if i != "" else np.nan for i in city["tempsMin"]
                ]
                lowest_temperature_uppers += [
                    i if i != "" else np.nan for i in city["tempsMinUpper"]
                ]
                lowest_temperature_lowers += [
                    i if i != "" else np.nan for i in city["tempsMinLower"]
                ]
                # 最高気温
                highest_temperatures += [
                    i if i != "" else np.nan for i in city["tempsMax"]
                ]
                highest_temperature_uppers += [
                    i if i != "" else np.nan for i in city["tempsMaxUpper"]
                ]
                highest_temperature_lowers += [
                    i if i != "" else np.nan for i in city["tempsMaxLower"]
                ]
            except Exception as e:
                logger.exception(f"city is {city}")

        week_temps_df = pd.DataFrame(
            {
                "city_code": city_codes,
                "city_name": city_names,
                "forecast_target_date": forecast_target_dates,
                "lowest_temperature": lowest_temperatures,
                "lowest_temperature_upper": lowest_temperature_uppers,
                "lowest_temperature_lower": lowest_temperature_lowers,
                "highest_temperature": highest_temperatures,
                "highest_temperature_upper": highest_temperature_uppers,
                "highest_temperature_lower": highest_temperature_lowers,
            }
        )

        # 取得日時
        week_temps_df["get_datetime"] = self.get_datetime
        # 気象情報レポート日時
        week_temps_df["report_datetime"] = report_datetime
        # 気象台名
        week_temps_df[
            "meteorological_observatory_name"
        ] = meteorological_observatory_name

        self.week_temps_df = week_temps_df[
            [
                "get_datetime",
                "report_datetime",
                "meteorological_observatory_name",
                "city_code",
                "city_name",
                "forecast_target_date",
                "lowest_temperature",
                "lowest_temperature_upper",
                "lowest_temperature_lower",
                "highest_temperature",
                "highest_temperature_upper",
                "highest_temperature_lower",
            ]
        ]

    def __extract_past_tempavg(
        self,
        past_tempavg_dict: dict[str, Any],
        report_datetime: datetime.datetime,
        meteorological_observatory_name: str,
    ):
        """
        向こう1週間の平年気温を抽出しDataFrameに格納する
        Args:
            fewdays_weather_dict: dict[str, Any]: 該当予報値辞書
            report_datetime: datetime.datetime: 気象情報レポート日時
            meteorological_observatory_name: str: 気象台名
        """

        city_codes: list[str] = []
        city_names: list[str] = []
        lowest_temperatures: list[float] = []
        highest_temperatures: list[float] = []

        # エリアごとの情報
        cities = past_tempavg_dict["areas"]

        for city in cities:
            try:
                city_codes += [city["area"]["code"]]
                city_names += [city["area"]["name"]]
                # 最低気温
                lowest_temperatures += [city["min"]]
                # 最高気温
                highest_temperatures += [city["max"]]
            except Exception as e:
                logger.exception(f"city is {city}")

        past_tempavg_df = pd.DataFrame(
            {
                "city_code": city_codes,
                "city_name": city_names,
                "lowest_temperature": lowest_temperatures,
                "highest_temperature": highest_temperatures,
            }
        )

        # 取得日時
        past_tempavg_df["get_datetime"] = self.get_datetime
        # 気象情報レポート日時
        past_tempavg_df["report_datetime"] = report_datetime
        # 気象台名
        past_tempavg_df[
            "meteorological_observatory_name"
        ] = meteorological_observatory_name

        self.past_tempavg_df = past_tempavg_df[
            [
                "get_datetime",
                "report_datetime",
                "meteorological_observatory_name",
                "city_code",
                "city_name",
                "lowest_temperature",
                "highest_temperature",
            ]
        ]

    def __extract_past_precipitationavg(
        self,
        past_precipitationavg_dict: dict[str, Any],
        report_datetime: datetime.datetime,
        meteorological_observatory_name: str,
    ):
        """
        向こう1週間の平年降水量を抽出しDataFrameに格納する
        Args:
            fewdays_weather_dict: dict[str, Any]: 該当予報値辞書
            report_datetime: datetime.datetime: 気象情報レポート日時
            meteorological_observatory_name: str: 気象台名
        """

        city_codes: list[str] = []
        city_names: list[str] = []
        precopitation_mins: list[float] = []
        precopitation_maxs: list[float] = []

        # エリアごとの情報
        cities = past_precipitationavg_dict["areas"]

        for city in cities:
            try:
                city_codes += [city["area"]["code"]]
                city_names += [city["area"]["name"]]
                # 最低気温
                precopitation_mins += [city["min"]]
                # 最高気温
                precopitation_maxs += [city["max"]]
            except Exception as e:
                logger.exception(f"city is {city}")

        past_precopitationavg_df = pd.DataFrame(
            {
                "city_code": city_codes,
                "city_name": city_names,
                "precopitation_min": precopitation_mins,
                "precopitation_max": precopitation_maxs,
            }
        )

        # 取得日時
        past_precopitationavg_df["get_datetime"] = self.get_datetime
        # 気象情報レポート日時
        past_precopitationavg_df["report_datetime"] = report_datetime
        # 気象台名
        past_precopitationavg_df[
            "meteorological_observatory_name"
        ] = meteorological_observatory_name

        self.past_precopitationavg_df = past_precopitationavg_df[
            [
                "get_datetime",
                "report_datetime",
                "meteorological_observatory_name",
                "city_code",
                "city_name",
                "precopitation_min",
                "precopitation_max",
            ]
        ]

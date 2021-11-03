import os
import pickle
from typing import Optional

import yaml

from utils import gcs

from logging import getLogger
from logging import DEBUG
from logging import StreamHandler
from logging import Formatter

logger = getLogger(__name__)
logger.setLevel(DEBUG)
handler = StreamHandler()
handler.setLevel(DEBUG)
logger.addHandler(handler)


def read_file(filepath: str) -> str:
    """
    ファイル読み込み
    """

    with open(filepath) as f:
        txt = f.read()
    return txt


def exists(filepath: str) -> bool:
    """
    ファイルやディレクトリの有無
    """

    return os.path.exists(filepath)


def delete_file(filepath: str):
    """
    ファイルの削除
    """
    if exists(filepath):
        os.remove(filepath)
        logger.info("local {} deleted".format(filepath))
    else:
        logger.warning("local {} not exist".format(filepath))
    return


def read_yaml(filepath: str):
    """
    Yamlファイル読み込み
    """
    with open(filepath) as file:
        yml = yaml.safe_load(file)
    return yml


def read_csvfile(
    filename: str,
    local_dir: str,
    bucket_name: Optional[str] = None,
    gcs_filename_prefix: Optional[str] = None,
    usecols: Optional[list[str]] = None,
    dtype: Optional[dict[str, str]] = None,
):
    """
    CSVファイルを読み込みDataFrameにする
    GCSから読み込む場合はダウンロードしてから読み込み
    ダウンロードしていてすでにローカルにある場合はbucket_nameをNoneにする(あまり変更がなく容量の大きいファイルのとき推奨)
    params:
        filename: str: 読み込みファイル名(パスではない)
        local_dir: str: 読み込み元ファイルがあるローカルディレクトリ(GCSからのダウンロード先)
        bucket_name: Optional[str]: バケット名
        gcs_filename_prefix: Optional[str]: GCSの対象ファイルのprefix(ファイル名直前まで)
        usecols: Optional[list[str]]: csvファイルの列指定
        dtype: Optional[dict[str, str]]: 読み込み時の型指定
    return:
        ファイルから読み込んだDataFrame
    """
    if bucket_name is not None:
        gcs.from_gcs(
            bucket_name=bucket_name,
            filepath=gcs_filename_prefix + filename,
            download_path=local_dir + filename,
        )
    return pd.read_csv(local_dir + filename, usecols=usecols, dtype=dtype)


def to_csvfile(
    df,
    filename: str,
    local_dir: str,
    bucket_name: Optional[str] = None,
    gcs_filename_prefix: Optional[str] = None,
    index: bool = False,
):
    """
    DataFrameをCSVファイルに出力する
    GCSへ出力する場合はcsv出力してからアップロード
    アップロードするほどでもない内容の場合はbucket_nameをNoneにする(容量の大きいファイルの一時ファイルとき推奨)
    params
        df: DataFrame
        filename: 書き込みファイル名(パスではない)
        local_dir: str: 書き込み元ファイルがあるローカルディレクトリ(GCSへのアップロード元)
        gcs_filename_prefix: Optional[str]: GCSのアップロード先のprefix(ファイル名直前まで)
        index: bool: DataFrameのindexも列として書き出すか
    """
    df.to_csv(local_dir + "/" + filename, index=index)

    if bucket_name is not None:
        gcs.to_gcs(
            bucket_name=bucket_name,
            filepath=gcs_filename_prefix + "/" + filename,
            upload_path=local_dir + "/" + filename,
        )
    return


def load_object(
    filename: str,
    local_dir: str,
    bucket_name: Optional[str] = None,
    gcs_filename_prefix: Optional[str] = None,
):
    """
    オブジェクトのファイルを読み込む
    GCSから読み込む場合はダウンロードしてから読み込み
    params:
        filename: str: 読み込みファイル名(パスではない)
        local_dir: str: 読み込み元ファイルがあるローカルディレクトリ(GCSからのダウンロード先)
        bucket_name: Optional[str]: バケット名
        gcs_filename_prefix: Optional[str]: GCSの対象ファイルのprefix(ファイル名直前まで)
    return:
        ファイルから読み込んだDataFrame
    """
    if bucket_name is not None:
        gcs.from_gcs(
            bucket_name=bucket_name,
            filepath=gcs_filename_prefix + filename,
            download_path=local_dir + filename,
        )
    # モデルのオープン
    with open(local_dir + filename, mode="rb") as f:
        obj = pickle.load(f)

    return obj


def save_object(
    obj,
    filename: str,
    local_dir: str,
    bucket_name: Optional[str] = None,
    gcs_filename_prefix: Optional[str] = None,
):
    """
    オブジェクトをファイルに出力
    GCSへ出力する場合は、出力してからアップロード
    params
        obj: DataFrame
        filename: 書き込みファイル名(パスではない)
        local_dir: str: 書き込み元ファイルがあるローカルディレクトリ(GCSへのアップロード元)
        bucket_name: Optional[str]: バケット名
        gcs_filename_prefix: Optional[str]: GCSのアップロード先のprefix(ファイル名直前まで)
    """

    with open(local_dir + filename, mode="wb") as f:  # with構文でファイルパスとバイナリ書き込みモードを設定
        pickle.dump(obj, f)

    if bucket_name is not None:
        gcs.to_gcs(
            bucket_name=bucket_name,
            filepath=gcs_filename_prefix + filename,
            upload_path=local_dir + filename,
        )
    return

import os

from utils import files


# config.yamlファイルを読み込むデコレータ
def set_config(func):
    def wrapper(*args, **kwargs):

        # config.yamlファイルを読み込む
        config = files.read_yaml("yamls/config.yaml")

        # 環境変数を読み込み
        config["env"] = os.environ.get("_ENV")
        config["project_id"] = os.environ.get("_PROJECT_ID")
        config["bucket_name"] = os.environ.get("_BUCKET_NAME")

        # デコレーションされる関数の実行
        result = func(config, *args, **kwargs)

        return result

    return wrapper

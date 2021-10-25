from google.cloud import bigquery
from google.cloud import bigquery_storage
from google.cloud.exceptions import NotFound
from logging import getLogger
from logging import DEBUG
from logging import StreamHandler
from logging import Formatter

logger = getLogger(__name__)
logger.setLevel(DEBUG)
handler = StreamHandler()
handler.setLevel(DEBUG)
logger.addHandler(handler)


def exe_query(query: str):
    """
    クエリを実行
    params
        query: str: 実行クエリ
    returns
        クエリ結果
    """

    try:
        client = bigquery.Client()

        query_job = client.query(query)

        results = query_job.result()

        return results
    except Exception as e:
        logger.exception("クエリ失敗")


def to_dataframe(query: str, use_bqstorage_api=False):
    """
    クエリ実行しDFへ持つ
    use_bqstorage_apiがTrueのときbigquery storage api を用いて高速に取得できる
    """

    client = bigquery.Client()
    return (
        client.query(query)
        .result()
        .to_dataframe(
            create_bqstorage_client=use_bqstorage_api,
        )
    )


def to_dataframe_using_pdgbq(
    query: str,
    project_id: str,
    progress_bar_type="tqdm_notebook",
    use_bqstorage_api=False,
):
    """
    クエリの実行しDFへ持つ
    use_bqstorage_apiがTrueのときbigquery storage api を用いて高速に取得できる
    """
    return pd.read_gbq(
        query,
        project_id,
        progress_bar_type=progress_bar_type,
        use_bqstorage_api=use_bqstorage_api,
    )


def from_dataframe_using_pdgbq(
    df, dataset_name: str, table_name: str, project_id: str, if_exists: str = "fail"
):
    """
    DFの内容をテーブルへ出力
    params
        df: DataFrame
        dataset_name: 出力先データセット名
        table_name: 出力先テーブル名
        project_id: BQのプロジェクトID
        if_exists: 'fail', 'replace', 'append'
    """

    df.to_gbq(
        dataset_name + "." + table_name,
        project_id=project_id,
        if_exists=if_exists,
    )


def exists_table(table_id):

    # テーブルの存在確認
    client = bigquery.Client()
    try:
        client.get_table(table_id)  # Make an API request.
        logger.info("Table {} already exists.".format(table_id))
        return True
    except NotFound:
        logger.info("Table {} is not found.".format(table_id))
        # ない
        return False

    except Exception as e:
        logger.exception("テーブル存在確認処理失敗")


def create_table(table_id: str, schema_path: str, partition_field=None):
    """
    テーブルを生成
    params
        table_id: str: {project_id}:{dataset}:{table}
        schema_path: str: スキーマ定義されたjsonファイルのパス
        partition_field: str: パーティション分割列の設定をする場合その列を指定
    """
    client = bigquery.Client()
    schema = client.schema_from_json(schema_path)
    table = bigquery.Table(table_id, schema=schema)
    if partition_field is not None:
        # パーティションの設定
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field=partition_field
        )

    # テーブル作成
    table = client.create_table(table)
    logger.info("created table")

    return


def insert_table(table_id, data):
    """
    テーブルに挿入する
    params:
        table_id: str: project_id.dataset_name.table_name
        data: list[dict]: insertするデータ
    """

    client = bigquery.Client()
    # テーブルに追加
    try:
        errors = client.insert_rows_json(table_id, data)  # Make an API request.
        if errors == []:
            logger.info("New rows have been added.")
        else:
            logger.error("Encountered errors while inserting rows: {}".format(errors))

    except Exception as e:
        logger.exception("テーブル挿入失敗")

        return


def file_to_table(
    project_id: str,
    dataset_name: str,
    table_name: str,
    table_schema_path: str,
    source_file_uri: str,
    replace: bool = False,
    partition_field: str = None,
    skip_leading_rows: int = 1,
):
    """
    csvデータをBQに取り込む処理
    params:
        project_id: str: プロジェクト名,
        dataset_name: str: データセット名,
        table_name: str: テーブル名,
        table_schema_path: スキーマ定義ファイルパス
        source_file_uri: str: 取り込み元ファイルURI,
        replace: bool: 置き換えるか否か(default: Flase)
        partition_field: str: パーティションフィールド指定
        skip_leading_rows: スキップ行数
    returns:
    """

    client = bigquery.Client()

    table_id = "{}.{}.{}".format(project_id, dataset_name, table_name)

    # 追記か置き換えか
    if replace:
        write_disposition = "WRITE_TRUNCATE"
    else:
        write_disposition = "WRITE_APPEND"

    schema = client.schema_from_json(table_schema_path)

    # パーティション列の指定があれば設定する
    if partition_field is None:
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            skip_leading_rows=skip_leading_rows,
            source_format=bigquery.SourceFormat.CSV,
            write_disposition=write_disposition,
        )
    else:
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            skip_leading_rows=skip_leading_rows,
            source_format=bigquery.SourceFormat.CSV,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partition_field,  # Name of the column to use for partitioning.
            ),
            write_disposition=write_disposition,
        )

    load_job = client.load_table_from_uri(
        source_file_uri, table_id, job_config=job_config
    )

    load_job.result()  # Wait for the job to complete.

    table = client.get_table(table_id)
    logger.info("Loaded {} rows to table {}".format(table.num_rows, table_id))

    return


def export_csv(
    destination_gs_uri,
    bq_project,
    bq_dataset_id,
    bq_table_id,
    compression=None,
    destination_format="CSV",
    print_header=True,
):
    """
    テーブルのcsvエクスポート
    """

    client = bigquery.Client()

    dataset_ref = bigquery.DatasetReference(bq_project, bq_dataset_id)
    table_ref = dataset_ref.table(bq_table_id)

    job_config = bigquery.ExtractJobConfig(
        compression=compression,
        destination_format=destination_format,
        print_header=print_header,
    )

    extract_job = client.extract_table(
        table_ref, destination_gs_uri, location="US", job_config=job_config
    )
    extract_job.result()  # Waits for job to complete.

    logger.info(
        "Exported {}:{}.{} to {}".format(
            bq_project, bq_dataset_id, bq_table_id, destination_gs_uri
        )
    )


def delete_table(project_id, dataset_name, table_name, not_found_ok=True):
    """
    テーブルの削除
    """

    client = bigquery.Client()

    table_id = f"{project_id}.{dataset_name}.{table_name}"

    client.delete_table(table_id, not_found_ok=not_found_ok)
    logger.info("Deleted table '{}'.".format(table_id))


def create_dataset(dataset_name):
    """
    データセットの作成
    params:
        dataset_name: str 作成するデータセット名
    """

    client = bigquery.Client()

    dataset_id = "{project_id}.{dataset_name}".format(
        project_id=client.project, dataset_name=dataset_name
    )

    dataset = bigquery.Dataset(dataset_id)

    dataset.location = "US"

    dataset = client.create_dataset(dataset, timeout=30)
    logger.info("Created dataset {}.{}".format(client.project, dataset.dataset_id))


def exists_dataset(dataset_name):
    """
    データセットの存在確認
    params:
        dataset_name: str 作成するデータセット名
    """

    client = bigquery.Client()

    dataset_id = "{project_id}.{dataset_name}".format(
        project_id=client.project, dataset_name=dataset_name
    )

    try:
        client.get_dataset(dataset_id)  # Make an API request.
        logger.info("Dataset {} already exists".format(dataset_id))
        return True
    except NotFound:
        logger.info("Dataset {} is not found".format(dataset_id))
        return False

    except Exception as e:
        logger.exception("データセット存在確認失敗")

    return


def delete_dataset(dataset_name):
    """
    データセットの削除
    """

    client = bigquery.Client()

    dataset_id = "{project_id}.{dataset_name}".format(
        project_id=client.project, dataset_name=dataset_name
    )

    client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)

    logger.info("Deleted dataset '{}'.".format(dataset_id))

    return

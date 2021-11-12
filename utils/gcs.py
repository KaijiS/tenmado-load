import logging

from google.cloud import storage

# loggerの設定
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def from_gcs(bucket_name: str, filepath: str, download_path: str):
    """
    GCSからダウンロード
    params:
        bucket_name: str: バケット名
        filepath: str: GCS上のダウンロードするファイルのパス
        download_path: str: ダウンロード先パス
    """

    client = storage.Client()

    bucket = client.get_bucket(bucket_name)

    blob = bucket.blob(filepath)

    blob.download_to_filename(download_path)


def to_gcs(bucket_name: str, filepath: str, upload_path: str):
    """
    GCSへアップロード
    params:
        bucket_name: str: バケット名
        filepath: str: GCS上のアップロード先ファイルのパス
        upload_path: str: アップロード対象ファイルパス
    """
    client = storage.Client()

    bucket = client.get_bucket(bucket_name)

    blob = bucket.blob(filepath)

    blob.upload_from_filename(upload_path)


def rename_blob(bucket_name: str, blob_name: str, new_name: str) -> None:
    """Renames a blob."""
    # bucket_name = "your-bucket-name"
    # blob_name = "your-object-name"
    # new_name = "new-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    new_blob = bucket.rename_blob(blob, new_name)

    logger.info("Blob {} has been renamed to {}".format(blob.name, new_blob.name))

    return


def copy_blob(
    bucket_name: str,
    blob_name: str,
    destination_bucket_name: str,
    destination_blob_name,
):
    # bucket_name = "your-bucket-name"
    # blob_name = "your-object-name"
    # destination_bucket_name = "destination-bucket-name"
    # destination_blob_name = "destination-object-name"

    storage_client = storage.Client()

    source_bucket = storage_client.bucket(bucket_name)
    source_blob = source_bucket.blob(blob_name)
    destination_bucket = storage_client.bucket(destination_bucket_name)

    blob_copy = source_bucket.copy_blob(
        source_blob, destination_bucket, destination_blob_name
    )

    logger.info(
        "Blob {} in bucket {} copied to blob {} in bucket {}.".format(
            source_blob.name,
            source_bucket.name,
            blob_copy.name,
            destination_bucket.name,
        )
    )
    return


def mv_blob(
    bucket_name: str,
    blob_name: str,
    destination_bucket_name: str,
    destination_blob_name: str,
) -> None:
    """Copies a blob from one bucket to another with a new name."""
    # bucket_name = "your-bucket-name"
    # blob_name = "your-object-name"
    # destination_bucket_name = "destination-bucket-name"
    # destination_blob_name = "destination-object-name"

    storage_client = storage.Client()

    source_bucket = storage_client.bucket(bucket_name)
    source_blob = source_bucket.blob(blob_name)
    destination_bucket = storage_client.bucket(destination_bucket_name)

    blob_copy = source_bucket.copy_blob(
        source_blob, destination_bucket, destination_blob_name
    )

    logger.info(
        "Blob {} in bucket {} copied to blob {} in bucket {}.".format(
            source_blob.name,
            source_bucket.name,
            blob_copy.name,
            destination_bucket.name,
        )
    )

    source_blob.delete()

    logger.info(
        "deleted source blob: {} in bucket {}.".format(
            source_blob.name, source_bucket.name
        )
    )

    return


def delete_blob(bucket_name, blob_name) -> None:
    """Deletes a blob from the bucket."""
    # bucket_name = "your-bucket-name"
    # blob_name = "your-object-name"

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    if blob.exists():
        blob.delete()
        logger.info("Blob {} deleted.".format(blob_name))
        return
    else:
        logger.warning("Blob {} is not exist".format(blob_name))

    return


def find_objects(bucket_name: str, prefix: str) -> list[str]:
    """
    指定したprefixのオブジェクトの一覧を返す
    params
        bucket_name: str: バケット名
        prefix: str: 指定するprefix
    return
        存在するかオブジェクト名のリスト
    """
    client = storage.Client()
    return [blob.name for blob in client.list_blobs(bucket_name, prefix=prefix)]


def delete_blobs(bucket_name: str, prefix: str) -> None:
    """
    指定したprefixのオブジェクトを全削除
    params
        bucket_name: str: バケット名
        prefix: str: 指定するprefix
    """

    blob_names = find_objects(bucket_name=bucket_name, prefix=prefix)
    for blob_name in blob_names:
        delete_blob(bucket_name=bucket_name, blob_name=blob_name)

    return


def exists_objects(bucket_name: str, prefix: str) -> bool:
    """
    指定したprefixのオブジェクトが存在するか
    params
        bucket_name: str: バケット名
        prefix: str: 指定するprefix
    return
        存在するか否か
    """

    object_names = find_objects(bucket_name=bucket_name, prefix=prefix)

    if len(object_names) == 0:
        return False

    return True


def mv_objects(bucket_name: str, prefix: str, source_key: str, target_key: str) -> None:
    """
    同じバケット内の指定したprefixのオブジェクトを指定した箇所に移動する
    renameすることで実現
    params
        params bucket_name: str: バケット名
        prefix: str: 指定するprefix
        source_key: str: rename元キーワード
        target_key: str: rename先キーワード
    return
    """

    for blob_name in find_objects(bucket_name=bucket_name, prefix=prefix):

        new_name = blob_name.replace(source_key, target_key)
        rename_blob(bucket_name=bucket_name, blob_name=blob_name, new_name=new_name)

    return


def create_bucket(bucket_name, storage_class="COLDLINE", location="us"):
    """Create a new bucket in specific location with storage class"""
    # bucket_name = "your-new-bucket-name"

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    bucket.storage_class = storage_class
    new_bucket = storage_client.create_bucket(bucket, location=location)

    logger.info(
        "Created bucket {} in {} with storage class {}".format(
            new_bucket.name, new_bucket.location, new_bucket.storage_class
        )
    )
    return new_bucket

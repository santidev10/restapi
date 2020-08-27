import logging

from django.conf import settings

from saas import celery_app
from utils.aws.s3_exporter import S3Exporter

logger = logging.getLogger(__name__)


class IASIngestor(S3Exporter):
    bucket_name = settings.AMAZON_S3_IAS_BUCKET_NAME
    export_content_type = "application/CSV"

    @classmethod
    def get_s3_key(cls, name):
        key = name
        return key

    @classmethod
    def list_objects(cls):
        s3 = cls._s3()
        return s3.list_objects_v2(
            Bucket=cls.bucket_name
        )


@celery_app.task
def ingest_ias_data():
    ingestor = IASIngestor()
    objects = ingestor.list_objects()
    try:
        contents = objects["Contents"]
        file_names = [content["Key"] for content in contents]
        for file_name in file_names:
            ias_content = ingestor._get_s3_object(name=file_name)
            for byte in ias_content["Body"].iter_lines():
                row = (byte.decode("utf-8")).split(",")
                cid = row[0].split("/")[-1]
                # todo: Store ingested data into Elastic Search models, then move file from s3 bucket into archive
    except Exception as e:
        logger.error(e)

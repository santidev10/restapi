import logging

from django.conf import settings
from django.utils import timezone

from audit_tool.models import AuditChannel
from audit_tool.models import IASChannel
from es_components.constants import Sections
from es_components.managers import ChannelManager
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
    channel_manager = ChannelManager(sections=(Sections.MAIN, Sections.IAS_DATA, Sections.CUSTOM_PROPERTIES),
                                     upsert_sections=(Sections.MAIN, Sections.IAS_DATA, Sections.CUSTOM_PROPERTIES))
    try:
        contents = objects["Contents"]
        file_names = [content["Key"] for content in contents]
        for file_name in file_names:
            if file_name == "archive/":
                continue
            ias_content = ingestor._get_s3_object(name=file_name)
            new_cids = []
            for byte in ias_content["Body"].iter_lines():
                row = (byte.decode("utf-8")).split(",")
                cid = row[0].split("/")[-1]
                if len(cid) < 24:
                    continue
                new_cids.append(cid)
            new_channels = channel_manager.get_or_create(new_cids)
            for channel in new_channels:
                if not channel:
                    continue
                channel_id = channel.meta.id
                channel.custom_properties.is_tracked = True
                channel.ias_data.ias_verified = timezone.now()
                ias_channel = IASChannel.get_or_create(channel_id=channel_id)
            channel_manager.upsert(new_channels)
            source_key = file_name
            dest_key = f"archive/{file_name}"
            ingestor.copy_from(source_key, dest_key)
            ingestor.delete_obj(source_key)
    except Exception as e:
        logger.error(e)

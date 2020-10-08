import logging

from django.conf import settings
from django.utils import timezone

from audit_tool.models import IASChannel
from audit_tool.models import IASHistory
from es_components.constants import Sections
from es_components.managers import ChannelManager
from saas import celery_app
from saas.configs.celery import TaskExpiration, TaskTimeout
from utils.aws.s3_exporter import S3Exporter
from utils.celery.tasks import lock

logger = logging.getLogger(__name__)

LOCK_NAME = "ingest_ias"


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


@celery_app.task(expires=TaskExpiration.INGEST_IAS, soft_time_limit=TaskTimeout.INGEST_IAS)
def ingest_ias_channels():
    try:
        lock(lock_name=LOCK_NAME, expire=TaskExpiration.INGEST_IAS)
        ingestor = IASIngestor()
        objects = ingestor.list_objects()
        channel_manager = ChannelManager(sections=(Sections.MAIN, Sections.IAS_DATA, Sections.CUSTOM_PROPERTIES),
                                         upsert_sections=(Sections.MAIN, Sections.IAS_DATA, Sections.CUSTOM_PROPERTIES))
        contents = objects["Contents"]
        file_names = [content["Key"] for content in contents]
        for file_name in file_names:
            if "archive" in file_name:
                continue
            try:
                ias_content = ingestor._get_s3_object(name=file_name)
                ias_history = IASHistory.objects.get_or_create(name=file_name)
                new_cids = []
                for byte in ias_content["Body"].iter_lines():
                    row = (byte.decode("utf-8")).split(",")
                    cid = row[0].split("/")[-1]
                    if len(cid) != 24 or cid[:2] != "UC":
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
                    ias_channel.ias_verified = timezone.now()
                    ias_channel.save(update_fields=["ias_verified"])
                channel_manager.upsert(new_channels)
                source_key = file_name
                dest_key = f"{settings.IAS_ARCHIVE_FOLDER}{file_name}"
                ingestor.copy_from(source_key, dest_key)
                if settings.ARCHIVE_IAS:
                    ingestor.delete_obj(source_key)
                ias_history.completed = timezone.now()
                ias_history.save()
            # pylint: disable=broad-except
            except Exception as e:
                # pylint: enable=broad-except
                logger.error(e)
                continue
    # pylint: disable=broad-except
    except Exception:
        # pylint: enable=broad-except
        pass

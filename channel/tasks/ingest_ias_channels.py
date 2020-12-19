import logging
from time import sleep

from django.conf import settings
from django.utils import timezone

from audit_tool.models import AuditChannel
from audit_tool.models import IASChannel
from audit_tool.models import IASHistory
from audit_tool.models import get_hash_name
from es_components.constants import Sections
from es_components.managers import ChannelManager
from saas import celery_app
from saas.configs.celery import TaskExpiration
from saas.configs.celery import TaskTimeout
from utils.aws.s3_exporter import S3Exporter
from utils.celery.tasks import lock
from utils.celery.tasks import unlock
from utils.utils import chunks_generator

logger = logging.getLogger(__name__)

LOCK_NAME = "ingest_ias"

# Main number of channel ids to process at a time
CHANNEL_ID_CHUNK_SIZE = 10000

# number of "sub chunks" to break each postgres filter (by id), bulk_create query into
# TODO uncomment
# POSTGRES_CHUNK_SIZE = 2000
POSTGRES_CHUNK_SIZE = 100


@celery_app.task(expires=TaskExpiration.INGEST_IAS, soft_time_limit=TaskTimeout.INGEST_IAS)
def ingest_ias_channels():
    """
    Celery task for running the IASChannelIngestor class
    :return:
    """
    lock(lock_name=LOCK_NAME, expire=TaskExpiration.INGEST_IAS)
    channel_ingestor = IASChannelIngestor()
    channel_ingestor.run()
    unlock(LOCK_NAME)


class IASS3Exporter(S3Exporter):
    """
    S3Exporter for interacting with S3 objects
    """
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


class IASChannelIngestor:
    """
    Extract YouTube Channel ids from CSVs in a specified S3 bucket. Creates PG AuditChannel, IASChannel records and
    ES Channel records
    """

    def __init__(self):
        self.s3 = IASS3Exporter()
        self.channel_manager = ChannelManager(
            sections=(Sections.MAIN, Sections.IAS_DATA, Sections.CUSTOM_PROPERTIES),
            upsert_sections=(Sections.MAIN, Sections.IAS_DATA, Sections.CUSTOM_PROPERTIES)
        )
        self.process_queue = []

    def run(self, file_name: str = None):
        """
        Main public method. Can run on a target ES file name
        :param file_name:
        :return:
        """
        self._init_process_queue(file_name)
        if not self.process_queue:
            logger.info("IAS Channel Ingestor has no files to process")
            return

        for file_name in self.process_queue:
            try:
                self._process_file(file_name)
            # pylint: disable=broad-except
            except Exception as e:
                # pylint: enable=broad-except
                logger.error(e)

    def _init_process_queue(self, file_name: str = None):
        """
        create a list of filenames to queue up for processing
        :param file_name:
        :return:
        """
        file_names = self._get_s3_file_names()
        if file_name and file_name in file_names:
            self.process_queue = [file_name]
        else:
            self.process_queue = [file_name for file_name in file_names if "archive" not in file_name]

    def _get_s3_file_names(self) -> list:
        """
        get a list of s3 filenames from the s3 bucket
        :return file_names: list
        """
        objects = self.s3.list_objects()
        contents = objects["Contents"]
        file_names = [content["Key"] for content in contents]
        return file_names

    def _process_file(self, file_name: str = None):
        """
        process file_name in the s3 bucket
        :param file_name:
        :return:
        """
        self._make_working_copy_of_s3_object(file_name)
        ias_history, _created = IASHistory.objects.update_or_create(name=file_name,
                                                                    defaults={"started": timezone.now()})

        channel_ids = self._get_channel_ids_from_file_name(file_name)
        for channel_ids_chunk in chunks_generator(channel_ids, size=CHANNEL_ID_CHUNK_SIZE):
            channel_ids_chunk = list(channel_ids_chunk)
            self.upsert_es_channels(channel_ids_chunk)
            self.upsert_pg_channels(channel_ids_chunk)
            # TODO exp. backoff here
            sleep(20)

        self._archive_s3_object(file_name)
        ias_history.completed = timezone.now()
        ias_history.save(update_fields=["completed"])

    def _make_working_copy_of_s3_object(self, file_name: str):
        """
        copy a file to the temporary/working/processing directory in s3
        :param file_name:
        :return:
        """
        source_key = file_name
        destination_key = f"{settings.IAS_PROCESSING_FOLDER}{file_name}"
        self.s3.copy_from(source_key, destination_key)

    def _get_channel_ids_from_file_name(self, file_name: str) -> list:
        """
        get validated channel ids from a specified s3 filename
        :param file_name: string
        :return: list
        """
        ias_content = self.s3._get_s3_object(name=file_name)
        channel_ids = []
        for byte in ias_content["Body"].iter_lines():
            row = (byte.decode("utf-8")).split(",")
            channel_id = row[0].split("/")[-1]
            if len(channel_id) != 24 or channel_id[:2] != "UC":
                continue
            channel_ids.append(channel_id)
        return channel_ids

    def upsert_es_channels(self, channel_ids: list):
        """
        given a list of channel ids, add missing channel records in elasticsearch
        :param channel_ids:
        :return:
        """
        es_channels = self.channel_manager.get_or_create(channel_ids)
        self.channel_manager.upsert(es_channels)

    def upsert_pg_channels(self, channel_ids: list):
        """
        given a list of channel ids, add missing AuditChannel and IASChannel records
        :param channel_ids:
        :return:
        """
        for yt_channel_ids_chunk in chunks_generator(channel_ids, size=POSTGRES_CHUNK_SIZE):
            yt_channel_ids_chunk = list(yt_channel_ids_chunk)

            # get diff between all relevant YouTube channel ids and existing AuditChannels to find which YouTube
            # channel ids we need to create new AuditChannel records for
            # TODO remove!
            # existing_audit_channels = AuditChannel.objects.filter(channel_id__in=yt_channel_ids_chunk)
            # existing_audit_channel_yt_ids = [audit_channel.channel_id for audit_channel in existing_audit_channels]
            existing_audit_channel_yt_ids = list(AuditChannel.objects.filter(channel_id__in=yt_channel_ids_chunk)
                                                 .values_list("channel_id", flat=True))
            # bulk create AuditChannels if AuditChannels are missing
            audit_channel_yt_ids_to_create = list(set(yt_channel_ids_chunk) - set(existing_audit_channel_yt_ids))
            if len(audit_channel_yt_ids_to_create):
                audit_channels_to_create = [AuditChannel(channel_id=yt_channel_id,
                                                         channel_id_hash=get_hash_name(yt_channel_id))
                                            for yt_channel_id in audit_channel_yt_ids_to_create]
                AuditChannel.objects.bulk_create(audit_channels_to_create)

            # get diff between all relevant AuditChannels and AuditChannels with attached IASChannels to find what
            # IASChannels we need to create (using the AuditChannel.id)
            all_audit_channel_ids = list(AuditChannel.objects.filter(channel_id__in=yt_channel_ids_chunk)
                                         .values_list("id", flat=True))
            attached_audit_channel_ids = list(IASChannel.objects.filter(channel_id__in=all_audit_channel_ids)
                                              .values_list("channel_id", flat=True))
            # TODO remove!
            # ias_channels = IASChannel.objects.filter(channel_id__in=all_audit_channel_ids).prefetch_related("channel")
            # attached_audit_channel_ids = [ias_channel.channel.id for ias_channel in ias_channels]
            # bulk create IASChannels if IASChannels are missing
            unattached_audit_channel_ids = list(set(all_audit_channel_ids) - set(attached_audit_channel_ids))
            if len(unattached_audit_channel_ids):
                now = timezone.now()
                ias_channels_to_create = [IASChannel(channel_id=channel_id, ias_verified=now)
                                          for channel_id in unattached_audit_channel_ids]
                IASChannel.objects.bulk_create(ias_channels_to_create)

    def _archive_s3_object(self, file_name: str, delete_from_processing=True):
        """
        copy a file to the archive directory in s3
        delete from processing directory by default
        deletes original file if settings.ARCHIVE_IAS is set
        :param file_name: string
        :param delete_from_processing: bool delete from the processing directory as well
        :return:
        """
        source_key = f"{settings.IAS_PROCESSING_FOLDER}{file_name}"
        destination_key = f"{settings.IAS_ARCHIVE_FOLDER}{file_name}"
        self.s3.copy_from(source_key, destination_key)
        if delete_from_processing:
            self.s3.delete_obj(source_key)
        if settings.ARCHIVE_IAS:
            self.s3.delete_obj(file_name)


# @celery_app.task(expires=TaskExpiration.INGEST_IAS, soft_time_limit=TaskTimeout.INGEST_IAS)
# def ingest_ias_channels(target_file_name=None):
#     try:
#         lock(lock_name=LOCK_NAME, expire=TaskExpiration.INGEST_IAS)
#         ingestor = IASS3Exporter()
#         objects = ingestor.list_objects()
#         channel_manager = ChannelManager(sections=(Sections.MAIN, Sections.IAS_DATA, Sections.CUSTOM_PROPERTIES),
#                                          upsert_sections=(Sections.MAIN, Sections.IAS_DATA, Sections.CUSTOM_PROPERTIES))
#         contents = objects["Contents"]
#         file_names = [content["Key"] for content in contents]
#         start_time = timezone.now()
#         for file_name in file_names:
#             if target_file_name:
#                 if target_file_name not in file_name:
#                     continue
#             else:
#                 if "archive" in file_name:
#                     continue
#             try:
#                 ias_content = ingestor._get_s3_object(name=file_name)
#                 new_cids = []
#                 for byte in ias_content["Body"].iter_lines():
#                     row = (byte.decode("utf-8")).split(",")
#                     cid = row[0].split("/")[-1]
#                     if len(cid) != 24 or cid[:2] != "UC":
#                         continue
#                     new_cids.append(cid)
#                 for channel_ids_chunk in chunks_generator(new_cids, size=10000):
#                     channel_ids_chunk = list(channel_ids_chunk)
#                     new_channels = channel_manager.get_or_create(channel_ids_chunk)
#                     for channel in new_channels:
#                         if not channel:
#                             continue
#                         channel_id = channel.meta.id
#                         channel.custom_properties.is_tracked = True
#                         channel.ias_data.ias_verified = timezone.now()
#                         ias_channel = IASChannel.get_or_create(channel_id=channel_id)
#                         ias_channel.ias_verified = timezone.now()
#                         ias_channel.save(update_fields=["ias_verified"])
#                     channel_manager.upsert(new_channels)
#                     sleep(20)
#                 source_key = file_name
#                 dest_key = f"{settings.IAS_ARCHIVE_FOLDER}{file_name}"
#                 ingestor.copy_from(source_key, dest_key)
#                 if settings.ARCHIVE_IAS:
#                     ingestor.delete_obj(source_key)
#                 ias_history = IASHistory.objects.create(name=file_name, completed=timezone.now())
#                 ias_history.started = start_time
#                 ias_history.save(update_fields=["started"])
#             # pylint: disable=broad-except
#             except Exception as e:
#                 # pylint: enable=broad-except
#                 logger.error(e)
#                 continue
#         unlock(LOCK_NAME)
#     # pylint: disable=broad-except
#     except Exception:
#         # pylint: enable=broad-except
#         pass

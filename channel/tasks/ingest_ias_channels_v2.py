import logging
from datetime import timedelta

from django.conf import settings
from django.db import transaction
from django.utils import timezone
from elasticapm.transport.exceptions import TransportException
from elasticsearch.exceptions import TransportError
from elasticsearch.helpers.errors import BulkIndexError
from es_components.constants import Sections
from es_components.managers import ChannelManager

from audit_tool.models import AuditChannel
from audit_tool.models import IASChannel
from audit_tool.models import IASHistory
from audit_tool.models import get_hash_name
from saas import celery_app
from saas.configs.celery import TaskExpiration
from saas.configs.celery import TaskTimeout
from utils.aws.s3_exporter import S3Exporter
from utils.celery.tasks import lock
from utils.celery.tasks import unlock
from utils.exception import backoff
from utils.utils import chunks_generator

logger = logging.getLogger(__name__)

LOCK_NAME = "ingest_ias"

# Main number of channel ids to process at a time
CHANNEL_ID_CHUNK_SIZE = 10000

# number of "sub chunks" to break each postgres filter (by id), bulk_create query into
POSTGRES_CHUNK_SIZE = 10000


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
        self.exceptions = []

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
                self.exceptions.append(e)
        # give me the exception type so i can catch it!
        if self.exceptions:
            exceptions = [f"{type(exception).__module__}.{type(exception).__qualname__}"
                          for exception in self.exceptions]
            logger.error(f"IAS Channel Ingestor has uncaught exceptions: {', '.join(exceptions)}")
            logger.error(self.exceptions)
            raise Exception(self.exceptions)

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
            # include only files in the top level cf-ias directory, files that are CSVs
            # and don't start with an "IGNORE" flag
            file_names = [file_name for file_name in file_names
                          if "/" not in file_name
                          and not file_name.startswith("IGNORE")
                          and file_name.endswith(".csv")]
            # Prevents RC from re-processing the same file when production doesn't archive items.
            if settings.IAS_SKIP_DUPLICATE_FILENAMES:
                file_names_to_skip = IASHistory.objects.filter(name__in=file_names).values_list("name", flat=True)
                file_names_to_skip = list(file_names_to_skip)
                to_skip_str = ", ".join(file_names_to_skip)
                logger.info(f"skipping files: {to_skip_str} because IAS_SKIP_DUPLICATE_FILENAMES is True and the file "
                            "names already exists in the history.")
                file_names = [file_name for file_name in file_names if file_name not in file_names_to_skip]

            self.process_queue = file_names

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

        # don't create a new history if there's a failed one (incomplete and started older than timeout)
        older_than_timeout = timezone.now() - timedelta(seconds=TaskTimeout.INGEST_IAS)
        ias_history = IASHistory.objects.filter(name=file_name, completed__isnull=True,
                                                started__lte=older_than_timeout).last()
        if not ias_history:
            ias_history = IASHistory.objects.create(name=file_name)

        channel_ids = self._get_channel_ids_from_file_name(file_name)
        for channel_ids_chunk in chunks_generator(channel_ids, size=CHANNEL_ID_CHUNK_SIZE):
            channel_ids_chunk = list(channel_ids_chunk)
            self._upsert_es_channels(channel_ids_chunk)
            self._upsert_pg_channels(channel_ids_chunk)

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

    # exp. backoff w/ noise, intended to catch ES query queue limit exceeded exception
    @backoff(max_backoff=60, exceptions=(TransportError, TransportException, BulkIndexError))
    def _upsert_es_channels(self, channel_ids: list):
        """
        given a list of channel ids, add missing channel records in elasticsearch. Update ALL given ids' ias_verified
        timestamp to now
        :param channel_ids:
        :return:
        """
        es_channels = self.channel_manager.get_or_create(channel_ids)
        now = timezone.now()
        for channel in es_channels:
            channel.custom_properties.is_tracked = True
            channel.ias_data.ias_verified = now
        self.channel_manager.upsert(es_channels)

    def _upsert_pg_channels(self, channel_ids: list):
        """
        given a list of channel ids, add missing AuditChannel and IASChannel records. Update ALL IASChannel records'
        ias_verified timestamps to now
        :param channel_ids:
        :return:
        """
        channel_ids = list(set(channel_ids))
        with transaction.atomic():
            for yt_channel_ids_chunk in chunks_generator(channel_ids, size=POSTGRES_CHUNK_SIZE):
                yt_channel_ids_chunk = list(yt_channel_ids_chunk)
                # get diff between all relevant YouTube channel ids and existing AuditChannels to find which YouTube
                # channel ids we need to create new AuditChannel records for
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
                attached_ias_channels = IASChannel.objects.filter(channel_id__in=all_audit_channel_ids)
                # update existing IASChannels' ias_verified timestamp to now
                attached_ias_channels.update(ias_verified=timezone.now())
                attached_audit_channel_ids = list(attached_ias_channels.values_list("channel_id", flat=True))
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

from datetime import timedelta
import random
from mock import patch

from django.utils import timezone

from audit_tool.models import IASChannel
from audit_tool.models import IASHistory
from audit_tool.models import AuditChannel
from audit_tool.models import get_hash_name
from channel.tasks.ingest_ias_channels_v2 import IASChannelIngestor
from es_components.tests.utils import ESTestCase
from utils.unittests.test_case import ExtendedAPITestCase


FILE_NAME = "file.csv"


CHANNEL_IDS = ["UCRI7hheejBbWS6etTNwMT0g", "UCZOlCVo53M355pFIsjWp3Ig", "UCtyKq8yAWqFZwY7qUdauIFA",
               "UCiJM1IEDi-RBPYfSRZapcpg", "UCOcFJfAUqS9SInynhj2pm1w", "UCcykZr4Xd86tW6uqGxJectA",
               "UCwlIUbhlimlpXfDcDFW9Asw", "UC9EtdoN22YDbV9QMH36t1eQ", "UCifaxjslmI28EZ9V1T9II1Q",
               "UC_5TjYhPRza9bB_EbWpRd3Q", "UCJmXLYHJI8VVgSzJ3ABNS5A", "UCK6MPLkl7yEgVxWP3cf2ujQ"]

INVALID_FILE_NAMES = ["some/file.csv", "IGNORE_this.csv", "should_not_be_processed"]
VALID_FILE_NAMES = ["should_be_processesed.csv", "also_valid.csv"]
FILE_NAMES = INVALID_FILE_NAMES + VALID_FILE_NAMES


def do_nothing(*args, **kwargs):
    pass


class IASChannelIngestorTestCase(ExtendedAPITestCase, ESTestCase):

    @patch.object(IASChannelIngestor, "_archive_s3_object", do_nothing)
    @patch.object(IASChannelIngestor, "_make_working_copy_of_s3_object", do_nothing)
    @patch.object(IASChannelIngestor, "_get_channel_ids_from_file_name", return_value=CHANNEL_IDS)
    @patch.object(IASChannelIngestor, "_get_s3_file_names", return_value=[FILE_NAME])
    def test_full_create_success(self, *args, **kwargs):
        channel_ingestor = IASChannelIngestor()
        channel_ingestor.run(file_name=FILE_NAME)

        ias_channels = IASChannel.objects.filter(channel__channel_id__in=CHANNEL_IDS)
        self.assertEqual(len(CHANNEL_IDS), ias_channels.count())
        self.assertEqual(IASHistory.objects.filter(name=FILE_NAME).count(), 1)

    @patch.object(IASChannelIngestor, "_archive_s3_object", do_nothing)
    @patch.object(IASChannelIngestor, "_make_working_copy_of_s3_object", do_nothing)
    @patch.object(IASChannelIngestor, "_get_channel_ids_from_file_name", return_value=CHANNEL_IDS)
    @patch.object(IASChannelIngestor, "_get_s3_file_names", return_value=[FILE_NAME])
    def test_partial_create_success(self, *args, **kwargs):
        """
        test that the correct number of linked IASChannel and AuditChannel pairs are created even when some records
        already exist
        :param args:
        :param kwargs:
        :return:
        """
        # create random number of existing AuditChannels that overlap the incoming "csv"
        audit_channel_create_count = random.randint(2, len(CHANNEL_IDS) - 1)
        audit_channel_ids = CHANNEL_IDS[:audit_channel_create_count]
        audit_channel_instances = [AuditChannel(channel_id=channel_id, channel_id_hash=get_hash_name(channel_id))
                                   for channel_id in audit_channel_ids]
        new_audit_channels = AuditChannel.objects.bulk_create(audit_channel_instances)

        # create random number of linked IASChannels from subset of existing AuditChannels
        ias_channel_create_count = random.randint(1, audit_channel_create_count - 1)
        now = timezone.now()
        verified_yesterday = now - timedelta(days=1)
        ias_channel_instances = [IASChannel(channel_id=audit_channel.id, ias_verified=verified_yesterday)
                                 for audit_channel in new_audit_channels[:ias_channel_create_count]]
        IASChannel.objects.bulk_create(ias_channel_instances)

        channel_ingestor = IASChannelIngestor()
        channel_ingestor.run(file_name=FILE_NAME)
        # check that all channels' ias_verified dates are updated
        ias_channels = IASChannel.objects.filter(channel__channel_id__in=CHANNEL_IDS, ias_verified__gte=now)
        self.assertEqual(len(CHANNEL_IDS), ias_channels.count())
        self.assertEqual(IASHistory.objects.filter(name=FILE_NAME).count(), 1)

    @patch.object(IASChannelIngestor, "_archive_s3_object", do_nothing)
    @patch.object(IASChannelIngestor, "_make_working_copy_of_s3_object", do_nothing)
    @patch.object(IASChannelIngestor, "_get_channel_ids_from_file_name", return_value=CHANNEL_IDS)
    @patch.object(IASChannelIngestor, "_get_s3_file_names", return_value=[FILE_NAME])
    def test_existing_failed_ias_history_updated(self, *args, **kwargs):
        """
        ensure we don't create duplicate IASHistory records for the same file name if the IASHistory is not completed
        :param args:
        :param kwargs:
        :return:
        """
        history = IASHistory.objects.create(name=FILE_NAME)
        history.started = timezone.now() - timedelta(days=1)
        history.save(update_fields=["started"])

        channel_ingestor = IASChannelIngestor()
        channel_ingestor.run(file_name=FILE_NAME)
        self.assertEqual(IASHistory.objects.filter(name=FILE_NAME).count(), 1)
        self.assertNotEqual(IASHistory.objects.get(pk=history.id).completed, history.completed)

    @patch.object(IASChannelIngestor, "_archive_s3_object", do_nothing)
    @patch.object(IASChannelIngestor, "_make_working_copy_of_s3_object", do_nothing)
    @patch.object(IASChannelIngestor, "_get_channel_ids_from_file_name", return_value=CHANNEL_IDS)
    @patch.object(IASChannelIngestor, "_get_s3_file_names", return_value=[FILE_NAME])
    def test_completed_ias_history_not_overwritten(self, *args, **kwargs):
        """
        ensure we create a duplicate IASHistory if one already completed
        :param args:
        :param kwargs:
        :return:
        """
        history = IASHistory.objects.create(name=FILE_NAME, started=timezone.now(), completed=timezone.now())
        channel_ingestor = IASChannelIngestor()
        channel_ingestor.run(file_name=FILE_NAME)
        self.assertEqual(IASHistory.objects.filter(name=FILE_NAME).count(), 2)
        self.assertNotEqual(IASHistory.objects.get(pk=history.id).completed, IASHistory.objects.last())


    @patch.object(IASChannelIngestor, "_get_s3_file_names", return_value=FILE_NAMES)
    def test_invalid_file_names_ignored(self, *args, **kwargs):
        channel_ingestor = IASChannelIngestor()
        channel_ingestor._init_process_queue()
        self.assertEqual(set(channel_ingestor.process_queue), set(VALID_FILE_NAMES))

from datetime import timedelta

from uuid import uuid4
from django.utils import timezone

import brand_safety.constants as constants
from segment.models import PersistentSegmentChannel
from segment.models import PersistentSegmentVideo
from segment.segment_list_generator import SegmentListGenerator

from django.test import TransactionTestCase


class PersistentSegmentCreationTestCase(TransactionTestCase):
    def test_should_not_update_master_channel_whitelist(self):
        list_generator = SegmentListGenerator(0)
        created_date = timezone.now() - timedelta(days=SegmentListGenerator.UPDATE_THRESHOLD - 1)
        segment = PersistentSegmentChannel.objects.create(
            title="test master channel",
            uuid=uuid4(),
            is_master=True,
            audit_category_id=None,
            category=constants.WHITELIST
        )
        PersistentSegmentChannel.objects.filter(id=segment.id).update(created_at=created_date)
        should_update = list_generator.check_should_update(PersistentSegmentChannel, True, None, constants.WHITELIST)
        self.assertEqual(should_update, False)

    def test_should_update_master_channel_whitelist(self):
        list_generator = SegmentListGenerator(0)
        created_date = timezone.now() - timedelta(days=SegmentListGenerator.UPDATE_THRESHOLD)
        segment = PersistentSegmentChannel.objects.create(
            title="test master channel",
            uuid=uuid4(),
            is_master=True,
            audit_category_id=None,
            category=constants.WHITELIST
        )
        PersistentSegmentChannel.objects.filter(id=segment.id).update(created_at=created_date)
        should_update = list_generator.check_should_update(PersistentSegmentChannel, True, None, constants.WHITELIST)
        self.assertEqual(should_update, True)

    def test_should_not_update_master_channel_blacklist(self):
        list_generator = SegmentListGenerator(0)
        created_date = timezone.now() - timedelta(days=SegmentListGenerator.UPDATE_THRESHOLD - 1)
        segment = PersistentSegmentChannel.objects.create(
            title="test master channel",
            uuid=uuid4(),
            is_master=True,
            audit_category_id=None,
            created_at=created_date,
            category=constants.BLACKLIST
        )
        PersistentSegmentChannel.objects.filter(id=segment.id).update(created_at=created_date)
        should_update = list_generator.check_should_update(PersistentSegmentChannel, True, None, constants.BLACKLIST)
        self.assertEqual(should_update, False)

    def test_should_update_master_channel_blacklist(self):
        list_generator = SegmentListGenerator(0)
        created_date = timezone.now() - timedelta(days=SegmentListGenerator.UPDATE_THRESHOLD)
        segment = PersistentSegmentChannel.objects.create(
            title="test master channel",
            uuid=uuid4(),
            is_master=True,
            audit_category_id=None,
            category=constants.BLACKLIST
        )
        PersistentSegmentChannel.objects.filter(id=segment.id).update(created_at=created_date)
        should_update = list_generator.check_should_update(PersistentSegmentChannel, True, None, constants.BLACKLIST)
        self.assertEqual(should_update, True)

    def test_not_should_update_master_video_whitelist(self):
        list_generator = SegmentListGenerator(0)
        created_date = timezone.now() - timedelta(days=SegmentListGenerator.UPDATE_THRESHOLD - 1)
        segment = PersistentSegmentVideo.objects.create(
            title="test master video",
            uuid=uuid4(),
            is_master=True,
            audit_category_id=None,
            category=constants.WHITELIST
        )
        PersistentSegmentVideo.objects.filter(id=segment.id).update(created_at=created_date)
        should_update = list_generator.check_should_update(PersistentSegmentVideo, True, None, constants.WHITELIST)
        self.assertEqual(should_update, False)

    def test_should_update_master_video_whitelist(self):
        list_generator = SegmentListGenerator(0)
        created_date = timezone.now() - timedelta(days=SegmentListGenerator.UPDATE_THRESHOLD)
        segment = PersistentSegmentVideo.objects.create(
            title="test master video",
            uuid=uuid4(),
            is_master=True,
            audit_category_id=None,
            category=constants.WHITELIST
        )
        PersistentSegmentVideo.objects.filter(id=segment.id).update(created_at=created_date)
        should_update = list_generator.check_should_update(PersistentSegmentVideo, True, None, constants.WHITELIST)
        self.assertEqual(should_update, True)

    def test_not_should_update_master_video_blacklist(self):
        list_generator = SegmentListGenerator(0)
        created_date = timezone.now() - timedelta(days=SegmentListGenerator.UPDATE_THRESHOLD - 1)
        segment = PersistentSegmentVideo.objects.create(
            title="test master video",
            uuid=uuid4(),
            is_master=True,
            audit_category_id=None,
            category=constants.BLACKLIST
        )
        PersistentSegmentVideo.objects.filter(id=segment.id).update(created_at=created_date)
        should_update = list_generator.check_should_update(PersistentSegmentVideo, True, None, constants.BLACKLIST)
        self.assertEqual(should_update, False)

    def test_should_update_master_video_blacklist(self):
        list_generator = SegmentListGenerator(0)
        created_date = timezone.now() - timedelta(days=SegmentListGenerator.UPDATE_THRESHOLD)
        segment = PersistentSegmentVideo.objects.create(
            title="test master video",
            uuid=uuid4(),
            is_master=True,
            audit_category_id=None,
            category=constants.BLACKLIST
        )
        PersistentSegmentVideo.objects.filter(id=segment.id).update(created_at=created_date)
        should_update = list_generator.check_should_update(PersistentSegmentVideo, True, None, constants.BLACKLIST)
        self.assertEqual(should_update, True)

    def test_should_update_channel_whitelist(self):
        list_generator = SegmentListGenerator(0)
        created_date = timezone.now() - timedelta(days=SegmentListGenerator.UPDATE_THRESHOLD)
        segment = PersistentSegmentChannel.objects.create(
            title="test channel",
            uuid=uuid4(),
            is_master=False,
            audit_category_id=0,
            category=constants.WHITELIST
        )
        PersistentSegmentChannel.objects.filter(id=segment.id).update(created_at=created_date)
        should_update = list_generator.check_should_update(PersistentSegmentChannel, False, segment.audit_category_id, constants.WHITELIST)
        self.assertEqual(should_update, True)

    def test_should_not_update_channel_whitelist(self):
        list_generator = SegmentListGenerator(0)
        created_date = timezone.now() - timedelta(days=SegmentListGenerator.UPDATE_THRESHOLD - 1)
        segment = PersistentSegmentChannel.objects.create(
            title="test channel",
            uuid=uuid4(),
            is_master=False,
            audit_category_id=0,
            category=constants.WHITELIST
        )
        PersistentSegmentChannel.objects.filter(id=segment.id).update(created_at=created_date)
        should_update = list_generator.check_should_update(PersistentSegmentChannel, False, segment.audit_category_id, constants.WHITELIST)
        self.assertEqual(should_update, False)

    def test_should_update_video_whitelist(self):
        list_generator = SegmentListGenerator(0)
        created_date = timezone.now() - timedelta(days=SegmentListGenerator.UPDATE_THRESHOLD)
        segment = PersistentSegmentVideo.objects.create(
            title="test video",
            uuid=uuid4(),
            is_master=False,
            audit_category_id=1,
            category=constants.WHITELIST
        )
        PersistentSegmentVideo.objects.filter(id=segment.id).update(created_at=created_date)
        should_update = list_generator.check_should_update(PersistentSegmentVideo, False, segment.audit_category_id, constants.WHITELIST)
        self.assertEqual(should_update, True)

    def test_should_not_update_video_whitelist(self):
        list_generator = SegmentListGenerator(0)
        created_date = timezone.now() - timedelta(days=SegmentListGenerator.UPDATE_THRESHOLD)
        segment = PersistentSegmentVideo.objects.create(
            title="test video",
            uuid=uuid4(),
            is_master=False,
            audit_category_id=1,
            category=constants.WHITELIST
        )
        PersistentSegmentVideo.objects.filter(id=segment.id).update(created_at=created_date)
        should_update = list_generator.check_should_update(PersistentSegmentVideo, False, segment.audit_category_id, constants.WHITELIST)
        self.assertEqual(should_update, True)

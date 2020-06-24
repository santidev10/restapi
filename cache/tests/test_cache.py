from time import sleep

from django.db import IntegrityError
from django.test import TestCase

from cache.models import CacheItem


class CacheTestCase(TestCase):
    def setUp(self):
        self.item_1 = CacheItem.objects.create(key="test1", value=dict(val="testval1"))
        self.item_2 = CacheItem.objects.create(key="test2", value=dict(val="testval2"))

    def test_unique_cache_key(self):
        with self.assertRaises(IntegrityError):
            CacheItem.objects.create(key="test1", value=dict())

    def test_cache_key_and_value(self):
        self.assertEqual(self.item_1.key, "test1")
        self.assertEqual(self.item_1.value, {"val": "testval1"})
        self.assertEqual(self.item_2.key, "test2")
        self.assertEqual(self.item_2.value, {"val": "testval2"})

    def test_cache_update_time_changes(self):
        updated_at_before = self.item_1.updated_at
        sleep(1)
        self.item_1.value["new_val"] = "newtestval"
        self.item_1.save()
        self.assertEqual(self.item_1.value, {
            "val": "testval1",
            "new_val": "newtestval"
        })
        self.assertNotEqual(updated_at_before, self.item_1.updated_at)

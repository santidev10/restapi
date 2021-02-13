from django.test import TestCase

from audit_tool.models import AuditLanguage
from brand_safety.models import BadWord
from brand_safety.models import BadWordCategory


class BadWordTestCase(TestCase):
    def setUp(self):
        self.category = BadWordCategory.objects.create(name="profanity")
        self.language = AuditLanguage.objects.create(name="test")

    def test_bad_word_manager(self):
        manager_active_only = BadWord.objects
        manager_all = BadWord.all_objects
        self.assertTrue(manager_active_only.active_only)
        self.assertFalse(manager_all.active_only)

    def test_single_soft_delete(self):
        _id = 1000
        test_word = BadWord.objects.create(id=_id, category=self.category, language=self.language)
        test_word.delete()
        soft_deleted = BadWord.all_objects.filter(id=_id)
        self.assertEqual(soft_deleted.exists(), True)
        self.assertEqual(soft_deleted[0].id, _id)
        self.assertEqual(soft_deleted[0].category.name, self.category.name)
        self.assertEqual(soft_deleted[0].language.name, self.language.name)
        self.assertIsNotNone(soft_deleted[0].deleted_at)
        # BadWord.objects should not return soft deleted items
        self.assertFalse(BadWord.objects.filter(id=_id).exists())

    def test_single_hard_delete(self):
        _id = 2000
        test_word = BadWord.objects.create(id=_id, category=self.category, language=self.language)
        test_word.hard_delete()
        self.assertFalse(BadWord.objects.filter(id=_id).exists())
        self.assertFalse(BadWord.all_objects.filter(id=_id).exists())

    def test_queryset_soft_delete(self):
        size = 10
        for i in range(size):
            BadWord.objects.create(id=i, name="test{}".format(i), category=self.category, language=self.language)
        BadWord.objects.all().delete()
        self.assertEqual(BadWord.objects.all().count(), 0)
        self.assertEqual(BadWord.all_objects.all().count(), size)
        all_words = BadWord.all_objects.all()
        self.assertTrue(all([word.category.name == self.category.name for word in all_words]))
        self.assertTrue(all([word.language.name == self.language.name for word in all_words]))
        self.assertTrue(all([word.deleted_at is not None for word in all_words]))

    def test_queryset_hard_delete(self):
        start = 50
        for i in range(start, start + 10):
            BadWord.objects.create(id=i, name="test{}".format(i), category=self.category, language=self.language)
        BadWord.objects.all().hard_delete()
        self.assertFalse(BadWord.objects.all().exists())
        self.assertFalse(BadWord.all_objects.all().exists())

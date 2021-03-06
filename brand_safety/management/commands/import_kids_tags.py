import csv
import logging

from django.core.management.base import BaseCommand

from audit_tool.models import AuditLanguage
from brand_safety.models import BadWord
from brand_safety.models import BadWordCategory
from utils.utils import remove_tags_punctuation

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            "--file_name",
            help="Filename of kids tags."
        )

    def handle(self, *args, **kwargs):
        file_name = kwargs["file_name"]
        with open(file_name, "r") as f:
            reader = csv.reader(f)
            category = BadWordCategory.objects.get(name="kids content")
            language = AuditLanguage.from_string("un")
            negative_score = 1
            counter = 0
            for row in reader:
                counter += 1
                print(f"Parsing row {counter}.")
                word = remove_tags_punctuation(row[0].lower().strip())
                if len(word) < 3:
                    continue
                try:
                    bad_word = BadWord.all_objects.get(name=word, language=language)
                    bad_word.deleted_at = None
                    bad_word.category = category
                    bad_word.negative_score = negative_score
                    bad_word.save()
                # pylint: disable=broad-except
                except Exception:
                # pylint: enable=broad-except
                    BadWord.objects.create(name=word, category=category, language=language,
                                           negative_score=negative_score)

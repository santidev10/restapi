import logging

from django.core.management.base import BaseCommand
import csv
from brand_safety.models import BadWord
from brand_safety.models import BadWordCategory
from audit_tool.models import AuditLanguage
from brand_safety.languages import LANG_CODES
from utils.utils import remove_tags_punctuation


logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            "--file_name",
            help="Manual brand safety scoring, should provide ids to update"
        )

    def handle(self, *args, **kwargs):
        file_name = kwargs["file_name"]

        BadWord.objects.all().delete()
        counter = 0
        with open(file_name, "r") as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                counter += 1
                print(f"Parsing Row: {counter}")
                try:
                    # Parse word
                    word = remove_tags_punctuation(row[0].lower().strip())
                    if len(word) < 3:
                        continue
                    # Parse category
                    category_string = row[1].lower().strip()
                    category = BadWordCategory.objects.get(name=category_string)
                    # Parse language
                    language_string = row[2].title()
                    lang_code = LANG_CODES[language_string]
                    language = AuditLanguage.objects.get(language=lang_code)
                    # Parse rating
                    negative_score = int(row[3])
                    if not word or not category or not language or not negative_score:
                        continue
                    try:
                        bad_word = BadWord.all_objects.get(name=word, language=language)
                        bad_word.deleted_at = None
                        bad_word.category = category
                        bad_word.negative_score = negative_score
                        bad_word.save()
                    except Exception as e:
                        BadWord.objects.create(name=word, category=category, language=language, negative_score=negative_score)
                except Exception as e:
                    continue

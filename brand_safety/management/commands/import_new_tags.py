import csv
import logging

from django.core.management.base import BaseCommand

from audit_tool.models import AuditLanguage
from brand_safety.languages import LANG_CODES
from brand_safety.models import BadWord
from brand_safety.models import BadWordCategory
from utils.lang import is_english
from utils.utils import remove_tags_punctuation

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            "--file_name",
            help="Filename of newly vetted tags."
        )

        parser.add_argument(
            "--delete_all",
            help="Set to True if you want to delete all tags in the database and reimport."
        )

        parser.add_argument(
            "--overwrite",
            help="Default value is False. Set to True if you want to overwrite duplicate tags when importing."
        )

    # pylint: disable=too-many-statements
    def handle(self, *args, **kwargs):
        file_name = kwargs["file_name"]
        invalid_rows_file_name = "invalid_new_tags.csv"
        invalid_rows = []
        try:
            if kwargs["delete_all"]:
                BadWord.objects.all().delete()
        # pylint: disable=broad-except
        except Exception:
        # pylint: enable=broad-except
            pass
        overwrite = bool(kwargs.get("overwrite", False))

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
                    if is_english(word) and len(word) < 3:
                        reason = [f"Word {word} is shorter than 3 English characters long when trimmed."]
                        invalid_rows.append(row + reason)
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
                        reason = ["One of word, category, language, or negative_score not found."]
                        invalid_rows.append(row + reason)
                        continue

                    if not overwrite:
                        try:
                            bad_word = BadWord.objects.get(name=word, language=language)
                            reason = [f"'overwrite' parameter is set to False, but the word '{word}' with language "
                                      f"'{language}' already exists in the database."]
                            invalid_rows.append(row + reason)
                            continue
                        # pylint: disable=broad-except
                        except Exception as e:
                        # pylint: enable=broad-except
                            pass

                    try:
                        bad_word = BadWord.all_objects.get(name=word, language=language)
                        bad_word.deleted_at = None
                        bad_word.category = category
                        bad_word.negative_score = negative_score
                        bad_word.save()
                    # pylint: disable=broad-except
                    except Exception as e:
                    # pylint: enable=broad-except
                        BadWord.objects.create(name=word, category=category, language=language,
                                               negative_score=negative_score)
                # pylint: disable=broad-except
                except Exception as e:
                # pylint: enable=broad-except
                    reason = [e]
                    invalid_rows.append(row + reason)
                    continue

        with open(invalid_rows_file_name, "w") as f2:
            writer = csv.writer(f2)
            for row in invalid_rows:
                writer.writerow(row)
    # pylint: enable=too-many-statements

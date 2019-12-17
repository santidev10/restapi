import csv
import string
from brand_safety.models import BadWord
from brand_safety.models import BadWordCategory
from audit_tool.models import AuditLanguage
from brand_safety.languages import LANG_CODES

file_name = "bste_tags_to_import/kids_content_tags.csv"

counter = 0
with open(file_name, "r") as f:
    reader = csv.reader(f)
    category = BadWordCategory.objects.get(name="kids content")
    language = AuditLanguage.from_string("un")
    negative_score = 1
    counter = 0
    for row in reader:
        counter += 1
        print(f"Parsing row {counter}.")
        word = row[0].translate(str.maketrans('', '', string.punctuation))
        try:
            bad_word = BadWord.all_objects.get(name=word, language=language)
            bad_word.deleted_at = None
            bad_word.category = category
            bad_word.negative_score = negative_score
            bad_word.save()
        except Exception as e:
            BadWord.objects.create(name=word, category=category, language=language, negative_score=negative_score)

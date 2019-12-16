import string
import csv
from brand_safety.models import BadWord
from brand_safety.models import BadWordCategory
from audit_tool.models import AuditLanguage
from brand_safety.languages import LANG_CODES

file_name = "bste_keywords_final_CLEAN_12_16_2019_1101am.csv"
invalid_rows_file_name = "invalid_keywords.csv"

invalid_rows = []


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
            word = row[0].translate(str.maketrans('', '', string.punctuation))
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
            try:
                bad_word = BadWord.all_objects.get(name=word, language=language)
                bad_word.deleted_at = None
                bad_word.category = category
                bad_word.negative_score = negative_score
                bad_word.save()
            except Exception as e:
                BadWord.objects.create(name=word, category=category, language=language, negative_score=negative_score)
        except Exception as e:
            reason = [e]
            invalid_rows.append(row + reason)
            continue

with open(invalid_rows_file_name, "w") as f2:
    writer = csv.writer(f2)
    for row in invalid_rows:
        writer.writerow(row)

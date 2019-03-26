from django.core.management import BaseCommand
from django.http import QueryDict

from brand_safety.models import BadWord
from singledb.connector import SingleDatabaseApiConnector


class Command(BaseCommand):
    def handle(self, *args, **options):
        bad_words_data = SingleDatabaseApiConnector().get_bad_words_list(QueryDict())
        bad_words = [
            BadWord(
                id=item["id"],
                name=item["name"],
                category=item["category"],
            )
            for item in bad_words_data
        ]
        BadWord.objects.all().delete()
        BadWord.objects.safe_bulk_create(bad_words)

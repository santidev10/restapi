from django.core.management.base import BaseCommand
from keyword_tool.settings import PREDEFINED_QUERIES


import logging
logger = logging.getLogger(__name__)


class Command(BaseCommand):

    def handle(self, *args, **options):
        for query in PREDEFINED_QUERIES:
            print(query)

import logging
from datetime import timedelta

from django.core.management.base import BaseCommand
from django.db.models import Q
from django.utils import timezone

from keyword_tool.models import Query, KeyWord

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def handle(self, *args, **options):
        expired_date = timezone.now().date() - timedelta(days=7)
        Query.objects.filter(updated_at__lte=expired_date).delete()
        self.delete_by_volume()

    def delete_by_volume(self):
        KeyWord.objects.filter(search_volume__lte=1000).delete()

    def delete_by_stats(self, expired_date):
        KeyWord.objects.filter(
            Q(updated_at__lte=expired_date) &
            (Q(average_cpc__isnull=True) | Q(average_cpc__lte=0)) &
            (Q(competition__isnull=True) | Q(competition__lte=0)) &
            (Q(search_volume__isnull=True) | Q(search_volume__lte=0))
        ).delete()

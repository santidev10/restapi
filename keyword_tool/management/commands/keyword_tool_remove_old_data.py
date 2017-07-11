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

        query_res = Query.objects.filter(updated_at__lte=expired_date).delete()
        keywords_res = KeyWord.objects.filter(
            Q(updated_at__lte=expired_date) &
            Q(average_cpc__gte=0) &
            Q(competition__gte=0) &
            Q(search_volume__gte=0)
        ).delete()

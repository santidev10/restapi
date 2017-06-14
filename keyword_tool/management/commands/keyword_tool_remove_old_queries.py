import logging
from datetime import timedelta

from django.core.management.base import BaseCommand
from django.utils import timezone

from keyword_tool.models import Query

logger = logging.getLogger(__name__)


class Command(BaseCommand):

    def handle(self, *args, **options):

        date = timezone.now().date() - timedelta(days=2)
        res = Query.objects.filter(updated_at__lte=date).delete()
        logger.info(res)



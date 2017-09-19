from django.core.management.base import BaseCommand

from payments.stripe_api import plans


class Command(BaseCommand):
    def handle(self, *args, **options):
        plans.sync_plans()

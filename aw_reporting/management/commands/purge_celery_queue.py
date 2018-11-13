from django.core.management import BaseCommand

from saas import celery_app


class Command(BaseCommand):

    def handle(self, *args, **options):
        celery_app.control.purge()

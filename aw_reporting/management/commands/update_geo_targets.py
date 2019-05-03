import logging

from django.core.management import BaseCommand

from aw_reporting.update.tasks.update_geo_targeting import update_geo_targeting

logger = logging.getLogger(__name__)

URL_PATTERN = "https://developers.google.com/adwords/api/docs/appendix/geo/geotargets-{date}.csv"


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            "--url",
            "-u",
            dest="url",
        )
        parser.add_argument(
            "--date",
            "-d",
            dest="url_date",
            default="2019-05-02",
        )

    def handle(self, *args, **options):
        logger.info("Start updating google geo targets")
        url_option = options.get("url")
        url_date = options.get("url_date")
        url = url_option if url_option else URL_PATTERN.format(date=url_date)
        update_geo_targeting(url)
        logger.info("End updating google geo targets")

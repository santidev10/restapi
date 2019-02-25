import logging

from django.core.management import BaseCommand

from audit_tool.adwords import AdwordsBlackList

logger = logging.getLogger(__name__)


class Command(BaseCommand):

    def handle(self, *args, **options) -> None:
        logger.info("Upload blacklists to AdWords")
        blacklist = AdwordsBlackList()
        blacklist.upload_master_blacklist()
        logger.info("Done")

from django.core.management.base import BaseCommand
from aw_reporting.tasks import check_users_to_aw_accounts_permissions
import logging

logging.basicConfig(format='%(asctime)s - %(message)s', level='INFO')
logger = logging.getLogger(__name__)


class Command(BaseCommand):

    def handle(self, *args, **options):
        check_users_to_aw_accounts_permissions()


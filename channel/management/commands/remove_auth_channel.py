import logging

from django.core.management.base import LabelCommand

from channel.actions import remove_auth_channel

logger = logging.getLogger(__name__)


class Command(LabelCommand):
    """
    Command to completely remove single or multimple youtube auth channels
    in test purpose.
    Ticket: https://channelfactory.atlassian.net/browse/SAAS-1733
    Summary: Ability to remove auth channel from DB
    """
    def handle_label(self, email, **options):
        remove_auth_channel(email)

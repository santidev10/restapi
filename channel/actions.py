import logging

from django.http import Http404

from singledb.connector import SingleDatabaseApiConnector
from userprofile.models import UserProfile, UserChannel

logger = logging.getLogger(__name__)


def remove_auth_channel(email):
    user = UserProfile.objects.filter(email=email)
    user_channels = UserChannel.objects.filter(user=user)
    connector = SingleDatabaseApiConnector()
    for channel in user_channels:
        try:
            print(connector)
            print(connector.delete_channel_test)
            connector.delete_channel_test(channel.channel_id)
        except Http404:
            logger.warning(
                "Failed to remove channel {channel_id} from single DB".format(
                    channel_id=channel.channel_id))

    user.delete()

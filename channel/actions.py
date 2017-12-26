from singledb.connector import SingleDatabaseApiConnector
from userprofile.models import UserProfile, UserChannel


def remove_auth_channel(email):
    user = UserProfile.objects.filter(email=email)
    user_channels = UserChannel.objects.filter(user=user)
    connector = SingleDatabaseApiConnector()
    for channel in user_channels:
        connector.delete_channel(channel.channel_id)

    user.delete()


def remove_user():
    pass

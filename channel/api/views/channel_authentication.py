import hashlib

import requests
from django.conf import settings
from django.contrib.auth import get_user_model
from django.utils import timezone
from oauth2client.client import OAuth2WebServerFlow
from rest_framework.response import Response
from rest_framework.status import HTTP_202_ACCEPTED
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_412_PRECONDITION_FAILED
from rest_framework.views import APIView

from administration.notifications import send_admin_notification
from administration.notifications import send_new_channel_authentication_email
from administration.notifications import send_welcome_email
from channel.models import AuthChannel
from es_components.constants import Sections
from es_components.managers.channel import ChannelManager
from es_components.managers.video import VideoManager
from userprofile.api.views.user_auth import UserAuthApiView
from userprofile.constants import UserStatuses
from userprofile.constants import UserTypeCreator
from userprofile.constants import StaticPermissions
from userprofile.models import PermissionItem
from userprofile.models import UserChannel
from userprofile.models import WhiteLabel
from utils.celery.dmp_celery import send_task_channel_general_data_priority
from utils.celery.dmp_celery import send_task_channel_stats_priority
from utils.es_components_cache import flush_cache
from utils.youtube_api import YoutubeAPIConnector

GOOGLE_API_TOKENINFO_URL_TEMPLATE = "https://www.googleapis.com/oauth2/v3/tokeninfo?access_token={}"


class ChannelAuthenticationApiView(APIView):
    permission_classes = tuple()

    def post(self, request, *args, **kwagrs):
        code = request.data.get("code")
        if not code:
            return Response(status=HTTP_400_BAD_REQUEST)

        try:
            credentials = self.get_credentials(code)
        # pylint: disable=broad-except
        except Exception:
            # pylint: enable=broad-except
            return Response(status=HTTP_400_BAD_REQUEST, data={"detail": "Invalid code"})

        youtube = YoutubeAPIConnector(access_token=credentials.access_token)
        api_data = youtube.own_channels(part="contentDetails")

        items = api_data.get("items", [])

        if not items:
            return Response(status=HTTP_400_BAD_REQUEST, data={"detail": "This account doesn't include any channels. "
                                                                         "Please try to authorize another YouTube "
                                                                         "account with channels."})

        channel_id = items[0].get("id")
        try:
            auth_channel = AuthChannel.objects.get(channel_id=channel_id)
            if auth_channel.token_revocation is not None:
                AuthChannel.objects \
                    .filter(channel_id=channel_id) \
                    .update(channel_id=channel_id,
                            refresh_token=credentials.refresh_token,
                            access_token=credentials.access_token,
                            client_id=settings.GOOGLE_APP_AUD,
                            client_secret=settings.GOOGLE_APP_SECRET,
                            access_token_expire_at=credentials.token_expiry,
                            token_revocation=None)
        except AuthChannel.DoesNotExist:
            if not credentials.refresh_token:
                return Response(status=HTTP_400_BAD_REQUEST, data={"detail": "No auth token"})
            channel = AuthChannel.objects.create(channel_id=channel_id,
                                                 refresh_token=credentials.refresh_token,
                                                 access_token=credentials.access_token,
                                                 client_id=settings.GOOGLE_APP_AUD,
                                                 client_secret=settings.GOOGLE_APP_SECRET,
                                                 access_token_expire_at=credentials.token_expiry)
            self.create_auth_channel(channel)
            send_admin_notification(channel_id)

        user, device_auth_token = self.get_or_create_user(credentials.access_token)

        if not user:
            return Response(status=HTTP_412_PRECONDITION_FAILED)

        if channel_id not in user.channels.values_list("channel_id", flat=True):
            UserChannel.objects.create(channel_id=channel_id, user=user)
            send_new_channel_authentication_email(user, channel_id, request)

        self.send_update_channel_tasks(channel_id)
        flush_cache()
        return Response(status=HTTP_202_ACCEPTED,
                        data={"auth_token": device_auth_token.key, "is_active": user.is_active})

    def create_auth_channel(self, auth_channel):
        manager = ChannelManager(Sections.AUTH)
        channel = manager.get_or_create([auth_channel.channel_id])[0]
        channel.populate_auth(updated_at=auth_channel.updated_at, created_at=auth_channel.created_at)
        manager.upsert([channel])

    def channel_remove_flag_deleted(self, channel_id):
        manager = ChannelManager(Sections.DELETED)
        channel = manager.get([channel_id]).pop()

        if channel and channel.deleted:
            manager.remove_sections(
                manager.ids_query([channel_id]),
                [Sections.DELETED]
            )

            video_manager = VideoManager()

            manager.remove_sections(
                video_manager.by_channel_ids_query(channel_id),
                [Sections.DELETED]
            )

    def send_update_channel_tasks(self, channel_id):
        self.channel_remove_flag_deleted(channel_id)
        send_task_channel_general_data_priority((channel_id,), wait=True)
        send_task_channel_stats_priority((channel_id,))

    def get_credentials(self, code):
        oauth2_flow = OAuth2WebServerFlow(
            client_id=settings.GOOGLE_APP_AUD,
            client_secret=settings.GOOGLE_APP_SECRET,
            scope=" ".join(settings.GOOGLE_APP_OAUTH2_SCOPES),
            redirect_uri=settings.GOOGLE_APP_OAUTH2_REDIRECT_URL,
            access_type="offline",
            response_type="code",
            approval_prompt="force",
            origin=settings.GOOGLE_APP_OAUTH2_ORIGIN)

        return oauth2_flow.step2_exchange(code)

    def get_or_create_user(self, access_token):
        """
        After successful channel authentication we create appropriate
         influencer user profile
        In case we've failed to create user - we send None
        :return: user instance or None
        """
        # If user is logged in we simply return it
        user = self.request.user
        if user and user.is_authenticated:
            self.set_user_avatar(user, access_token)
            device_auth_token = UserAuthApiView.create_device_auth_token(user)
            return user, device_auth_token

        # Starting user create procedure
        token_info_url = GOOGLE_API_TOKENINFO_URL_TEMPLATE.format(access_token)
        try:
            response = requests.get(token_info_url)
        # pylint: disable=broad-except
        except Exception:
            # pylint: enable=broad-except
            return None
        if response.status_code != 200:
            return None

        # Have successfully got basic user data
        response = response.json()
        email = response.get("email")
        try:
            user = get_user_model().objects.get(email=email)
            self.set_user_avatar(user, access_token)
        except get_user_model().DoesNotExist:
            google_id = response.get("sub")
            # Obtaining user extra data
            user_data = self.obtain_extra_user_data(access_token, google_id)
            domain = WhiteLabel.extract_sub_domain(self.request.get_host() or "")
            domain_obj = WhiteLabel.get(domain)

            # Authentication through Google OAuth should not grant Managed Service permissions
            disabled_managed_service_perms = {
                perm_name: False for perm_name in PermissionItem.all_perms()
                if StaticPermissions.MANAGED_SERVICE in perm_name
            }
            user_data.update(dict(
                email=email,
                google_account_id=google_id,
                status=UserStatuses.PENDING.value,
                is_active=False,
                user_type=UserTypeCreator.CREATOR.value,
                password=hashlib.sha1(str(timezone.now().timestamp()).encode()).hexdigest(),
                domain_id=domain_obj.id,
                perms=disabled_managed_service_perms,
            ))
            user = get_user_model().objects.create(**user_data)
            user.set_password(user.password)

            # Get or create auth token instance for user
            send_welcome_email(user, self.request)
        device_auth_token = UserAuthApiView.create_device_auth_token(user)
        return user, device_auth_token

    def obtain_extra_user_data(self, token, user_id):
        """
        Get user profile extra fields from user info
        :param token: oauth2 access token
        :param user_id: google user id
        :return: image link, name
        """
        user_data = {
            "first_name": "",
            "last_name": "",
            "profile_image_url": None,
            "last_login": timezone.now()
        }
        try:
            response_data = self.call_people_google_api(user_id, token, ["photos", "names"])
        # pylint: disable=broad-except
        except Exception:
            # pylint: enable=broad-except
            return user_data
        photos = response_data.get("photos", [])
        user_profile_image_url = self.obtain_user_avatar_from_response(photos)
        if user_profile_image_url:
            user_data["profile_image_url"] = user_profile_image_url
        names = response_data.get("names")
        if names:
            user_data["first_name"] = names[0].get("givenName", "")
            user_data["last_name"] = names[0].get("familyName", "")
        return user_data

    def set_user_avatar(self, user, access_token):
        """
        Obtain user avatar from google+
        """
        token_info_url = GOOGLE_API_TOKENINFO_URL_TEMPLATE.format(access_token)
        # --> obtain token info
        try:
            response = requests.get(token_info_url)
        # pylint: disable=broad-except
        except Exception:
            # pylint: enable=broad-except
            return
        if response.status_code != 200:
            return
        # <-- obtain token info
        # --> obtain user from people google
        response = response.json()
        user_google_id = response.get("sub")
        try:
            response_data = self.call_people_google_api(user_google_id, access_token, ["photos"])
        # pylint: disable=broad-except
        except Exception:
            # pylint: enable=broad-except
            return
        # <-- obtain user from people google
        # --> set user avatar
        photos = response_data.get("photos", [])
        profile_image_url = self.obtain_user_avatar_from_response(photos)
        if profile_image_url:
            user.profile_image_url = profile_image_url
            user.save()
        # <-- set user avatar
        return

    def call_people_google_api(self, user_google_id, access_token, fields, api_version="v1"):
        url = "https://people.googleapis.com/{}/people/{}/?access_token={}&personFields={}".format(
            api_version, user_google_id, access_token, ",".join(fields))
        response = requests.get(url)
        return response.json()

    def obtain_user_avatar_from_response(self, photos):
        """
        :param photos: google api object from response data
        :return: str or None
        """
        for photo in photos:
            metadata = photo.get("metadata", {})
            if metadata.get("primary", False) and metadata.get("source", {}).get("type") == "PROFILE":
                profile_image_url = photo.get("url", "").replace("s100", "s250")  # change avatar size
                return profile_image_url
        return None

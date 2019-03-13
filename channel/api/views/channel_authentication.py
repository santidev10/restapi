import hashlib

import requests
from django.contrib.auth import get_user_model
from django.utils import timezone
from rest_framework.authtoken.models import Token
from rest_framework.response import Response
from rest_framework.status import HTTP_202_ACCEPTED
from rest_framework.status import HTTP_408_REQUEST_TIMEOUT
from rest_framework.status import HTTP_412_PRECONDITION_FAILED
from rest_framework.views import APIView

from administration.notifications import send_new_channel_authentication_email
from administration.notifications import send_welcome_email
from segment.models import SegmentChannel
from segment.models import SegmentKeyword
from segment.models import SegmentVideo
from singledb.connector import SingleDatabaseApiConnector as Connector
from singledb.connector import SingleDatabaseApiConnectorException
from userprofile.constants import UserStatuses
from userprofile.constants import UserTypeCreator
from userprofile.models import UserChannel
from userprofile.models import get_default_accesses
from userprofile.permissions import PermissionGroupNames


class ChannelAuthenticationApiView(APIView):
    permission_classes = tuple()

    def post(self, request, *args, **kwagrs):
        connector = Connector()
        try:
            data = connector.auth_channel(request.data)
        except SingleDatabaseApiConnectorException as e:
            if e.sdb_response is not None:
                return Response(
                    data=e.sdb_response.json(),
                    status=e.sdb_response.status_code
                )
            data = {"error": " ".join(e.args)}
            return Response(data=data, status=HTTP_408_REQUEST_TIMEOUT)

        if data is not None:
            channel_id = data.get('channel_id')
            if channel_id:
                user, created = self.get_or_create_user(
                    data.get("access_token"))
                if not user:
                    return Response(status=HTTP_412_PRECONDITION_FAILED)

                user_channels = user.channels.values_list('channel_id',
                                                          flat=True)
                if channel_id not in user_channels:
                    UserChannel.objects.create(channel_id=channel_id,
                                               user=user)
                    send_new_channel_authentication_email(
                        user, channel_id, request)
                # set user avatar
                if not created:
                    self.set_user_avatar(user, data.get("access_token"))

                return Response(status=HTTP_202_ACCEPTED,
                                data={"auth_token": user.auth_token.key, "is_active": user.is_active})

        return Response()

    def get_or_create_user(self, access_token):
        """
        After successful channel authentication we create appropriate
         influencer user profile
        In case we've failed to create user - we send None
        :return: user instance or None
        """
        created = False
        # If user is logged in we simply return it
        user = self.request.user
        if user and user.is_authenticated():
            return user, created

        # Starting user create procedure
        token_info_url = "https://www.googleapis.com/oauth2/v3/tokeninfo" \
                         "?access_token={}".format(access_token)
        try:
            response = requests.get(token_info_url)
        except Exception:
            return None, created
        if response.status_code != 200:
            return None, created
        # Have successfully got basic user data
        response = response.json()
        email = response.get("email")
        try:
            user = get_user_model().objects.get(email=email)
        except get_user_model().DoesNotExist:
            google_id = response.get("sub")
            # Obtaining user extra data
            user_data = self.obtain_extra_user_data(access_token, google_id)
            # Create new user
            user_data["email"] = email
            user_data["google_account_id"] = google_id
            user_data["status"] = UserStatuses.PENDING.value
            user_data["is_active"] = False
            user_data["user_type"] = UserTypeCreator.CREATOR.value
            user_data["password"] = hashlib.sha1(str(
                timezone.now().timestamp()).encode()).hexdigest()
            user = get_user_model().objects.create(**user_data)
            user.set_password(user.password)

            # new default access implementation
            for group_name in get_default_accesses(via_google=True):
                user.add_custom_user_group(group_name)

            # Get or create auth token instance for user
            Token.objects.get_or_create(user=user)
            created = True
            send_welcome_email(user, self.request)
            self.check_user_segment_access(user)
        return user, created

    def check_user_segment_access(self, user):
        channel_segment_email_lists = SegmentChannel.objects.filter(shared_with__contains=[user.email]).exists()
        video_segment_email_lists = SegmentVideo.objects.filter(shared_with__contains=[user.email]).exists()
        keyword_segment_email_lists = SegmentKeyword.objects.filter(shared_with__contains=[user.email]).exists()
        if any([channel_segment_email_lists, video_segment_email_lists, keyword_segment_email_lists]):
            user.add_custom_user_group(PermissionGroupNames.MEDIA_PLANNING)

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
        except Exception as e:
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
        token_info_url = "https://www.googleapis.com/oauth2/v3/tokeninfo" \
                         "?access_token={}".format(access_token)
        # --> obtain token info
        try:
            response = requests.get(token_info_url)
        except Exception:
            return
        if response.status_code != 200:
            return
        # <-- obtain token info
        # --> obtain user from people google
        response = response.json()
        user_google_id = response.get("sub")
        try:
            response_data = self.call_people_google_api(user_google_id, access_token, ["photos"])
        except Exception as e:
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

    def call_people_google_api(self,  user_google_id, access_token, fields, api_version="v1"):
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

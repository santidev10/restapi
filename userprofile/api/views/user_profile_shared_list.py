from itertools import chain

from rest_framework.response import Response
from rest_framework.views import APIView

from segment.models import SegmentChannel
from segment.models import SegmentKeyword
from segment.models import SegmentVideo
from userprofile.models import UserProfile


class UserProfileSharedListApiView(APIView):
    def get(self, request):
        user = request.user
        response = []

        # filter all user segments
        channel_segment_email_lists = SegmentChannel.objects.filter(
            owner=user).values_list('shared_with', flat=True)
        video_segment_email_lists = SegmentVideo.objects.filter(
            owner=user).values_list('shared_with', flat=True)
        keyword_segment_email_lists = SegmentKeyword.objects.filter(
            owner=user).values_list('shared_with', flat=True)

        # build unique emails set
        unique_emails = set()
        for item in [channel_segment_email_lists, video_segment_email_lists,
                     keyword_segment_email_lists]:
            unique_emails |= set(chain.from_iterable(item))

        # collect required user data for each email
        for email in unique_emails:
            user_data = {}
            try:
                user = UserProfile.objects.get(email=email)
                user_data['email'] = user.email
                user_data['username'] = "{} {}".format(user.first_name,
                                                       user.last_name)
                user_data['first_name'] = user.last_name
                user_data['last_name'] = user.first_name
                user_data['registered'] = True
                user_data['date_joined'] = user.date_joined
                user_data['last_login'] = user.last_login

            except UserProfile.DoesNotExist:
                user_data['email'] = email
                user_data['registered'] = False

            response.append(user_data)

        return Response(data=response)

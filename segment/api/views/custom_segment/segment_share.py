import os
from email.mime.image import MIMEImage

from django.conf import settings
from django.core.mail import EmailMultiAlternatives
from django.template.loader import render_to_string
from django.utils.html import strip_tags
from rest_framework.generics import RetrieveUpdateDestroyAPIView
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_403_FORBIDDEN

from segment.api.mixins import DynamicModelViewMixin
from segment.api.serializers import SegmentSerializer
from userprofile.models import UserProfile
from userprofile.permissions import PermissionGroupNames


class SegmentShareApiView(DynamicModelViewMixin, RetrieveUpdateDestroyAPIView):
    serializer_class = SegmentSerializer

    def put(self, request, *args, **kwargs):
        segment = self.get_object()
        user = request.user
        shared_with = request.data.get('shared_with')

        # reject if request user has no permissions
        if not (user.is_staff or segment.owner == user):
            return Response(status=HTTP_403_FORBIDDEN)

        # send invitation email if user exists, else send registration email
        self.proceed_emails(segment, shared_with)

        # return saved segment
        serializer_context = {"request": request}
        response_data = self.serializer_class(
            segment,
            context=serializer_context
        ).data
        return Response(data=response_data, status=HTTP_200_OK)

    def proceed_emails(self, segment, emails):
        sender = settings.SENDER_EMAIL_ADDRESS
        message_from = self.request.user.get_full_name()
        exist_emails = segment.shared_with
        host = self.request.get_host()
        subject = "ViewIQ > You have been added as collaborator"
        segment_url = "https://{host}/media_planning/{segment_type}s/{segment_id}"\
                      .format(host=host,
                              segment_type=segment.segment_type,
                              segment_id=segment.id)
        context = dict(
            host=host,
            message_from=message_from,
            segment_url=segment_url,
            segment_title=segment.title,
        )
        # collect only new emails for current segment
        emails_to_iterate = [e for e in emails if e not in exist_emails]

        for email in emails_to_iterate:
            try:
                user = UserProfile.objects.get(email=email)
                context['name'] = user.get_full_name()
                to = user.email
                html_content = render_to_string(
                    "new_enterprise_collaborator.html",
                    context
                )
                self.send_email(html_content, subject, sender, to)
                # provide access to segments for collaborator
                user.add_custom_user_group(PermissionGroupNames.MEDIA_PLANNING)

            except UserProfile.DoesNotExist:
                to = email
                html_content = render_to_string(
                    "new_collaborator.html",
                    context)
                self.send_email(html_content, subject, sender, to)

        # update collaborators list
        segment.shared_with = emails
        segment.save()

    def send_email(self, html_content, subject, sender, to):
        text = strip_tags(html_content)
        msg = EmailMultiAlternatives(subject, text, sender, [to])
        msg.attach_alternative(html_content, "text/html")
        for f in ['bg.png', 'cf_logo_wt_big.png', 'img.png', 'logo.gif']:
            fp = open(os.path.join('segment/templates/', f), 'rb')
            msg_img = MIMEImage(fp.read())
            fp.close()
            msg_img.add_header('Content-ID', '<{}>'.format(f))
            msg.attach(msg_img)
        msg.send()

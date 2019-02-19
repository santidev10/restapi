"""
Administration notifications module
"""
import json
import os
import re
from email.mime.image import MIMEImage
from logging import Filter
from logging import Handler

import requests
from django.conf import settings
from django.core.mail import EmailMultiAlternatives
from django.core.mail import send_mail
from django.template.loader import get_template

from utils.lang import get_request_prefix

IGNORE_EMAILS_TEMPLATE = {
    "@pages.plusgoogle.com"
}
EMAIL_IMAGES_DIR = os.path.join(settings.STATIC_ROOT,
                                'img/notifications')


def send_new_registration_email(email_data):
    """
    Send new user registration email
    """
    sender = settings.SENDER_EMAIL_ADDRESS
    to = settings.REGISTRATION_ACTION_EMAIL_ADDRESSES
    subject = "New registration"
    text = "Dear Admin, \n\n" \
           "A new user has just registered on {host}. \n\n" \
           "User email: {email} \n" \
           "User first_name: {first_name} \n" \
           "User last_name: {last_name} \n" \
           "User company: {company}\n" \
           "User phone: {phone} \n"\
           "Annual ad spend: {annual_ad_spend} \n"\
           "User type: {user_type} \n\n"\
           "Please accept the user: {user_list_link} \n\n".format(**email_data)
    send_mail(subject, text, sender, to, fail_silently=True)
    return


def send_new_channel_authentication_email(user, channel_id, request):
    """
    Send new channel authentication email
    """
    sender = settings.SENDER_EMAIL_ADDRESS
    to = settings.CHANNEL_AUTHENTICATION_ACTION_EMAIL_ADDRESSES
    subject = "New channel authentication"
    host = request.get_host()
    prefix = get_request_prefix(request)
    text = "Dear Admin, \n\n" \
           "A user has just authenticated a channel on {host}. \n\n" \
           "User email: {email} \n" \
           "User first_name: {first_name} \n" \
           "User last_name: {last_name} \n" \
           "Channel id: {channel_id}\n" \
           "Link to channel: {link} \n\n" \
           "Please accept the user: {user_list_link} \n\n" \
        .format(
            host=request.get_host(),
            email=user.email,
            first_name=user.first_name, last_name=user.last_name,
            channel_id=channel_id,
            link="{}{}/research/channels/{}".format(prefix, host, channel_id),
            user_list_link="{}{}/admin/users".format(prefix, host),
        )
    send_mail(subject, text, sender, to, fail_silently=True)


def send_welcome_email(user, request):
    """
    Send welcome email to user
    """
    if user.email in IGNORE_EMAILS_TEMPLATE:
        return
    subject = "Welcome to {}".format(request.get_host())
    to = user.email
    text_header = "Dear {},\n\n".format(user.get_full_name())
    text_content = "Congratulations!" \
                   " You've just registered on {}.\n\n" \
                   "Kind regards\n" \
                   "Channel Factory Team".format(request.get_host())
    send_html_email(subject, to, text_header, text_content, request.get_host())


def send_html_email(subject, to, text_header, text_content, host):
    """
    Send email with html
    """
    sender = settings.SENDER_EMAIL_ADDRESS
    html = get_template("main.html")
    context = {"text_header": text_header,
               "text_content": text_content,
               "host": host}
    html_content = html.render(context=context)

    msg = EmailMultiAlternatives(subject, "{}{}".format(
        text_header, text_content), sender, [to])
    msg.attach_alternative(html_content, "text/html")
    msg.mixed_subtype = "related"

    for img in ["cf_logo_wt_big.png", "img.png", "logo.gif"]:
        img_path = os.path.join(EMAIL_IMAGES_DIR, img)
        with open(img_path, 'rb') as fp:
            msg_img = MIMEImage(fp.read())
        msg_img.add_header('Content-ID', '<{}>'.format(img))
        msg.attach(msg_img)

    msg.send(fail_silently=True)


class SlackAWUpdateLoggingHandler(Handler):
    slack_color_map = {
        "INFO": "good",
        "WARNING": "warning",
        "ERROR": "danger",
        "CRITICAL": "danger",
    }

    def emit(self, record):
        webhook_name = settings.AW_UPDATE_SLACK_WEBHOOK_NAME
        level_name = record.levelname
        slack_message_color = self.slack_color_map[level_name]
        log_entry = self.format(record)
        payload = {
            "attachments": [
                {
                    "pretext": "AdWords update on host: {}".format(settings.HOST),
                    "text": log_entry,
                    "color": slack_message_color,
                }
            ]
        }
        headers = {"Content-Type": "application/json"}
        timeout = 60
        requests.post(
            settings.SLACK_WEBHOOKS.get(webhook_name),
            data=json.dumps(payload),
            timeout=timeout,
            headers=headers,
        )


class Levels:
    WARNING = "WARNING"


class NotFoundWarningLoggingFilter(Filter):
    pattern = None

    def filter(self, record):
        assert self.pattern is not None,\
            "You must set sting with a regular expression in the 'patter' attribute of a child class"
        return not (record.levelname == Levels.WARNING and bool(re.match(self.pattern, record.msg)))


class AudienceNotFoundWarningLoggingFilter(NotFoundWarningLoggingFilter):
    pattern = "Audience \d+ not found"


class TopicNotFoundWarningLoggingFilter(NotFoundWarningLoggingFilter):
    pattern = "topic not found: \D+"


class UndefinedCriteriaWarningLoggingFilter(NotFoundWarningLoggingFilter):
    pattern = "Undefined criteria = \D+\d+"

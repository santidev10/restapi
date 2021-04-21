"""
Administration notifications module
"""
import json
import os
import re
import smtplib
from botocore.exceptions import ClientError
from logging import Filter
from logging import Handler
from re import Pattern

import requests
from django.conf import settings
from django.core import mail
from django.core.mail import send_mail
from django.template.loader import get_template

from utils.es_components_exporter import ESDataS3ExportApiView
from utils.lang import get_request_prefix

IGNORE_EMAILS_TEMPLATE = {
    "@pages.plusgoogle.com"
}
EMAIL_IMAGES_DIR = os.path.join(settings.STATIC_ROOT, "img/notifications")


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
           "User phone: {phone} \n" \
           "Annual ad spend: {annual_ad_spend} \n" \
           "User type: {user_type} \n\n" \
           "Please accept the user: {user_list_link} \n\n".format(**email_data)
    send_email(subject, text, sender, to, fail_silently=True)


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
    send_email(subject, text, sender, to, fail_silently=True)


def send_admin_notification(channel_id):
    sender = settings.SENDER_EMAIL_ADDRESS
    to = settings.CHANNEL_AUTHENTICATION_NOTIFY_TO
    subject = "Channel Authentication"
    message = f"Dear Admin, A new channel {channel_id} " \
              f"(https://www.viewiq.com/research/channels/{channel_id}) " \
              f"has just authenticated on ViewIQ"

    send_email(subject, message, sender, to, fail_silently=False)


def send_html_email(subject, to, text_header, text_content, from_email=None, fail_silently=False, host=None):
    """
    Send email with html
    """
    host = host or settings.HOST
    html_email = generate_html_email(text_header, text_content, host)
    send_email(
        subject=subject,
        recipient_list=[to] if isinstance(to, str) else to,
        html_message=html_email,
        from_email=from_email or settings.SENDER_EMAIL_ADDRESS,
        fail_silently=fail_silently
    )


def send_email(*_, subject, message=None, from_email=None, recipient_list, **kwargs):
    result = None
    if from_email is None or recipient_list is None:
        return result
    try:
        kwargs["fail_silently"] = False
        result = send_mail(subject=subject,
                           message=message,
                           from_email=from_email or settings.SENDER_EMAIL_ADDRESS,
                           recipient_list=recipient_list,
                           **kwargs)
    except (smtplib.SMTPException, ClientError):
        html_message = None
        if "html_message" in kwargs:
            html_message = kwargs["html_message"]
        result = send_email_using_alternative_smtp(subject, message, recipient_list, html_message)

    return result


def send_email_using_alternative_smtp(subject, message=None, recipient_list=None, html_message=None):
    result = None
    email_backend = 'django.core.mail.backends.smtp.EmailBackend'
    smtp_host = os.getenv("SMTP_HOST", "")
    smtp_email = os.getenv("SMTP_EMAIL", "")
    smtp_password = os.getenv("SMTP_PASSWORD", "")

    if not smtp_host or not smtp_email or not smtp_password:
        return result
    with mail.get_connection(backend=email_backend, fail_silently=True,
                             host=smtp_host, username=smtp_email, password=smtp_password,
                             port=465, use_ssl=True) as connection:
        result = send_mail(subject=subject, message=message, from_email=smtp_email, fail_silently=True,
                           recipient_list=recipient_list, html_message=html_message, connection=connection)
    return result


def send_welcome_email(user, request):
    """
    Send welcome email to user
    """
    if user.email in IGNORE_EMAILS_TEMPLATE:
        return
    host_address = ESDataS3ExportApiView.get_host_link(request)
    subject = "Welcome to {}".format(request.get_host())
    to = user.email
    text_header = "Dear {},\n\n".format(user.get_full_name())
    text_content = "Thank you for registering on ViewIQ! \n" \
                   "We will review your account and send you an update " \
                   "by email as soon as access is granted."
    send_html_email(subject, to, text_header, text_content, fail_silently=True, host=host_address)


def generate_html_email(text_header, text_content, host):
    """
    Generate email html with ChannelFactory template
    :param text_header:
    :param text_content:
    :param host:
    :return:
    """
    html = get_template("main_v3.html")
    context = {"text_header": text_header,
               "text_content": text_content,
               "host": host}
    html_content = html.render(context=context)
    return html_content


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
                    "pretext": "Update on host: {}".format(settings.HOST),
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


class SlackBrandSafetyLoggingHandler(Handler):
    slack_color_map = {
        "DEBUG": "good",
        "INFO": "good",
        "WARNING": "warning",
        "ERROR": "danger",
        "CRITICAL": "danger",
    }

    def emit(self, record):
        webhook_name = settings.BRAND_SAFETY_SLACK_WEBHOOK_NAME
        level_name = record.levelname
        slack_message_color = self.slack_color_map[level_name]
        log_entry = self.format(record)
        payload = {
            "attachments": [
                {
                    "pretext": "Brand safety on host: {}".format(settings.HOST),
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
    pattern: Pattern = None

    def filter(self, record):
        assert self.pattern is not None, \
            "You must set sting with a regular expression in the 'patter' attribute of a child class"
        return not (record.levelname == Levels.WARNING and bool(re.match(self.pattern, record.msg)))


class AudienceNotFoundWarningLoggingFilter(NotFoundWarningLoggingFilter):
    pattern = r"Audience \d+ not found"


class TopicNotFoundWarningLoggingFilter(NotFoundWarningLoggingFilter):
    pattern = r"topic not found: \D+"


class UndefinedCriteriaWarningLoggingFilter(NotFoundWarningLoggingFilter):
    pattern = r"Undefined criteria = \D+\d+"

"""
Administration notifications module
"""
from django.conf import settings
from django.core.mail import send_mail
from django.template.loader import get_template
from django.core.mail import EmailMultiAlternatives
from django.contrib.staticfiles.templatetags.staticfiles import static

IGNORE_EMAILS_TEMPLATE = {
    "@pages.plusgoogle.com"
}


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
           "User phone: {phone} \n\n".format(**email_data)
    send_mail(subject, text, sender, to, fail_silently=True)
    return


def send_welcome_email(user, request):
    """
    Send welcome email to user
    """
    if user.email in IGNORE_EMAILS_TEMPLATE:
        return
    subject = "Welcome to {}".format(request.get_host())
    to = user.email
    text_header = "Dear {},\n\n".format(user.get_full_name())
    text_content = "Congratulation!" \
                   " You've just registered on {}\n\n" \
                   "Kind regards\n" \
                   "Channel Factory Team".format(
                        user.get_full_name(), request.get_host())
    send_html_email(subject, to, text_header, text_content)


def send_html_email(subject, to, text_header, text_content):
    """
    Send email with html
    """
    sender = settings.SENDER_EMAIL_ADDRESS
    html = get_template("main.html")
    context = {"text_header": text_header,
               "text_content": text_content}
    html_content = html.render(context=context)
    html_content = html_content.replace(
        "cf_logo_wt_big.png",
        static("img/notifications/cf_logo_wt_big.png")).replace(
        "img.png", static("img/notifications/img.png")).replace(
        "logo.gif", static("img/notifications/logo.gif")).replace(
        "bg.png", static("img/notifications/bg.png"))
    msg = EmailMultiAlternatives(subject, "{}{}".format(
        text_header, text_content), sender, [to])
    msg.attach_alternative(html_content, "text/html")
    msg.send()

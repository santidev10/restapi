"""
Administration notifications module
"""
from django.conf import settings
from django.core.mail import send_mail


def send_new_registration_email(email_data):
    """
    Send new user registration email
    """
    sender = settings.SENDER_EMAIL_ADDRESS
    emails = "heorhii.pylypenko@sigma.software," \
             "anna.chumak@sigma.software," \
             "alexandra.vodopyanova@sigma.software"
    to = emails.split(",")
    subject = "New registration"
    text = "Dear Admin, \n\n" \
           "A new user has just registered on {host}. \n\n" \
           "User email: {email} \n" \
           "User company: {company}\n" \
           "User phone: {phone} \n\n".format(**email_data)
    send_mail(subject, text, sender, to, fail_silently=True)
    return

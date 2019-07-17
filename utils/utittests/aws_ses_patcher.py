def ses_monkey_patch():
    import utils.aws.ses_emailer
    from django.conf import settings
    from django.core.mail import EmailMultiAlternatives

    def send_mail(subject, to, text_header, text_content, host=None):
        sender = settings.SENDER_EMAIL_ADDRESS
        msg = EmailMultiAlternatives(subject, "{}{}".format(text_header, text_content), sender, [to])
        msg.send(fail_silently=True)

    utils.aws.ses_emailer.SESEmailer.send_email = send_mail

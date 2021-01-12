from django.conf import settings

from administration.notifications import send_html_email


def send_export_email(export_type, recipients, url):
    subject = f"PerformIQ {export_type} Analysis"
    text_header = f"Your {export_type} analysis is ready to view"
    text_content = f"<a href={url}>[Click here]</a> to review your report."
    send_html_email(
        subject=subject,
        to=recipients,
        text_header=text_header,
        text_content=text_content,
        from_email=settings.EXPORTS_EMAIL_ADDRESS
    )

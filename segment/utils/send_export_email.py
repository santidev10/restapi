from django.conf import settings

from administration.notifications import send_html_email


def send_export_email(recipients, export_title, download_url):
    subject = "Custom Target List: {}".format(export_title)
    text_header = "Your Custom Target List {} is ready".format(export_title)
    text_content = "<a href={download_url}>Click here to download</a>".format(download_url=download_url)
    send_html_email(
        subject=subject,
        to=recipients,
        text_header=text_header,
        text_content=text_content,
        from_email=settings.EXPORTS_EMAIL_ADDRESS
    )

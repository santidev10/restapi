import boto3
from botocore.exceptions import ClientError

from django.conf import settings

class SESEmailer(object):
    AWS_REGION = "us-east-1"
    CHARSET = "UTF-8"
    BODY_HTML = """<html>
    <head></head>
    <body>
    <p>{}</p>
    </body>
    </html>
    """
    aws_access_key_id = settings.AMAZON_S3_ACCESS_KEY_ID
    aws_secret_access_key = settings.AMAZON_S3_SECRET_ACCESS_KEY

    @classmethod
    def _ses(cls):
        ses = boto3.client(
            "ses",
            region_name=cls.AWS_REGION,
            aws_access_key_id=cls.aws_access_key_id,
            aws_secret_access_key=cls.aws_secret_access_key
        )
        return ses

    @classmethod
    def send_email(cls, sender, recipient, subject, body_text, body_html=None):
        if body_html is None:
            body_html = cls.BODY_HTML.format(body_text)
        ses = cls._ses()
        try:
            ses.send_email(
                Destination={
                    'ToAddresses': [
                        recipient,
                    ],
                },
                Message={
                    'Body': {
                        'Html': {
                            'Charset': cls.CHARSET,
                            'Data': body_html,
                        },
                        'Text': {
                            'Charset': cls.CHARSET,
                            'Data': body_text,
                        },
                    },
                    'Subject': {
                        'Charset': cls.CHARSET,
                        'Data': subject,
                    },
                },
                Source=sender,
            )
        except ClientError as e:
            raise ClientError

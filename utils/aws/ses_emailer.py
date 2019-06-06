import boto3
from botocore.exceptions import ClientError
from django.core.exceptions import ValidationError

from django.conf import settings


class SESEmailer(object):
    AWS_REGION = "us-east-1"
    CHARSET = "UTF-8"
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
    def send_email(cls, sender, recipient, subject, body_html):
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
                    },
                    'Subject': {
                        'Charset': cls.CHARSET,
                        'Data': subject,
                    },
                },
                Source=sender,
            )
        except ClientError:
            raise ValidationError("Failed to send email. Either the sender {} does not have AWS SES permissions,"
                                  "or the recipient {} is an invalid email address.".format(sender, recipient))
        except Exception as e:
            raise e

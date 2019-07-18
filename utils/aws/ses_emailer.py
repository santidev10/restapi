import boto3
from botocore.exceptions import ClientError
from django.core.exceptions import ValidationError

from django.conf import settings


class SESEmailer(object):
    AWS_REGION = "us-east-1"
    CHARSET = "UTF-8"
    aws_access_key_id = settings.AMAZON_S3_ACCESS_KEY_ID
    aws_secret_access_key = settings.AMAZON_S3_SECRET_ACCESS_KEY

    def __init__(self):
        self.ses = boto3.client(
            "ses",
            region_name=self.AWS_REGION,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key
        )

    def send_email(self, recipients, subject, body_html):
        if not isinstance(recipients, list) and isinstance(recipients, str):
            recipients = [recipients]
        try:
            self.ses.send_email(
                Destination={
                    'ToAddresses': recipients,
                },
                Message={
                    'Body': {
                        'Html': {
                            'Charset': self.CHARSET,
                            'Data': body_html,
                        },
                    },
                    'Subject': {
                        'Charset': self.CHARSET,
                        'Data': subject,
                    },
                },
                Source=settings.SENDER_EMAIL_ADDRESS,
            )
        except ClientError:
            raise ValidationError("Failed to send email. Either the sender ({}) or the "
                                  "recipient ({}) is an invalid email address.".format(host, recipients))
        except Exception as e:
            raise e

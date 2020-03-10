from django.conf import settings

from utils.aws.s3 import get_s3_client


def upload_file(filename, data, content_type, bucket=settings.AMAZON_S3_BUCKET_NAME, extra=None):
    s3 = get_s3_client()
    s3.put_object(
        Bucket=bucket,
        Key=filename,
        Body=data,
        ContentType=content_type,
        **extra or {}
    )
    return "https://{bucket}.s3.amazonaws.com/{filename}".format(bucket=bucket, filename=filename)

import os

import boto3


def get_settings_file_body():
    s3_settings_location = os.getenv("S3_SETTINGS", None)

    if s3_settings_location is None:
        raise FileNotFoundError()

    s3 = boto3.resource("s3")
    bucket, path = s3_settings_location.split(":")
    obj = s3.Object(bucket, path)
    return obj.get()["Body"].read().decode()

# pylint: disable=exec-used
exec(get_settings_file_body())
# pylint: enable=exec-used

del get_settings_file_body

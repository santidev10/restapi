def get_settings_file_body():
    import os

    s3_settings_location = os.getenv("S3_SETTINGS", None)

    if s3_settings_location is None:
        raise FileNotFoundError()

    import boto3
    s3 = boto3.resource("s3")
    bucket, path = s3_settings_location.split(":")
    obj = s3.Object(bucket, path)
    return obj.get()['Body'].read().decode()


exec(get_settings_file_body())

del get_settings_file_body

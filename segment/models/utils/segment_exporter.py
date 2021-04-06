from django.conf import settings
from django.utils import timezone

from segment.models.utils.export_context_manager import ExportContextManager
from utils.aws.s3_exporter import S3Exporter
from utils.utils import validate_youtube_url


class SegmentExporter(S3Exporter):
    S3Exporter.bucket_name = settings.AMAZON_S3_CUSTOM_SEGMENTS_BUCKET_NAME
    aws_access_key_id = settings.AMAZON_S3_ACCESS_KEY_ID
    aws_secret_access_key = settings.AMAZON_S3_SECRET_ACCESS_KEY

    def __init__(self, segment, bucket_name=None):
        self.segment = segment
        S3Exporter.bucket_name = bucket_name or S3Exporter.bucket_name

    def get_s3_key(self):
        raise NotImplementedError("Method should be defined segment models")

    def export_to_s3(self, segment, s3_key, queryset=None, extra_args=None):
        extra_args = extra_args or {}
        with ExportContextManager(segment=segment, queryset=queryset) as exported_file_name:
            self._s3().upload_file(
                Bucket=self.bucket_name,
                Key=s3_key,
                Filename=exported_file_name,
                ExtraArgs=extra_args,
            )

    def export_file_to_s3(self, filename, s3_key, extra_args=None):
        extra_args = extra_args or {}
        self._s3().upload_file(
            Bucket=self.bucket_name,
            Key=s3_key,
            Filename=filename,
            ExtraArgs=extra_args,
        )

    def export_file(self, s3_key=None, updating=False, queryset=None):
        now = timezone.now()
        export = self.segment.export
        if s3_key is None:
            s3_key = self.segment.get_s3_key()
        if updating:
            export.updated_at = now
        else:
            export.completed_at = now
        self.export_to_s3(self, s3_key, queryset=queryset)
        download_url = self.generate_temporary_url(s3_key, time_limit=3600 * 24 * 7)
        export.download_url = download_url
        export.save()

    def get_export_lines_stream(self, s3_key):
        export_content = self._get_s3_object(s3_key, get_key=False)
        for byte in export_content["Body"].iter_lines():
            row = byte.decode("utf-8").split(",")
            yield row

    def get_extract_export_ids(self, s3_key=None):
        """
        Parse and extract Channel or video ids from csv export
        :return:
        """
        if s3_key is None:
            s3_key = self.segment.get_s3_key()
        # pylint: disable=protected-access
        export_content = self._get_s3_object(s3_key, get_key=False)
        # pylint: enable=protected-access
        url_index = None
        for byte in export_content["Body"].iter_lines():
            row = (byte.decode("utf-8")).split(",")
            if url_index is None:
                try:
                    url_index = row.index("URL")
                    continue
                except ValueError:
                    url_index = 0
            item_id = validate_youtube_url(row[url_index], self.segment.segment_type)
            yield item_id

    def delete_export(self, s3_key=None):
        """
        Delete csv from s3
        :param s3_key: str -> S3 file keyname
        :return:
        """
        if s3_key is None:
            s3_key = self.segment.get_s3_key()
        self.delete_obj(s3_key)

    def get_export_file(self, s3_key):
        export_content = self.get_s3_export_content(s3_key, get_key=False).iter_chunks()
        return export_content

    def check_key_size(self, s3_key):
        response = self._s3().head_object(Bucket=self.bucket_name, Key=s3_key)
        size = response["ContentLength"]
        return size

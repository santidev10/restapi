from distutils.util import strtobool
import csv
import requests
import os
from uuid import uuid4

from audit_tool.models import AuditCategory
from audit_tool.models import AuditExporter
from audit_tool.models import AuditVideoProcessor
from audit_tool.models import AuditVideoMeta
from audit_tool.models import AuditChannelProcessor
from audit_tool.models import AuditChannelMeta
from audit_tool.models import AuditProcessor

from rest_framework.views import APIView
from rest_framework.exceptions import ValidationError

from rest_framework.response import Response
from django.conf import settings
from utils.aws.s3_exporter import S3Exporter
import boto3
from botocore.client import Config

class AuditExportApiView(APIView):
    CATEGORY_API_URL = "https://www.googleapis.com/youtube/v3/videoCategories" \
                       "?key={key}&part=id,snippet&id={id}"
    DATA_API_KEY = settings.YOUTUBE_API_DEVELOPER_KEY

    def get(self, request):
        query_params = request.query_params
        audit_id = query_params["audit_id"] if "audit_id" in query_params else None
        clean = query_params["clean"] if "clean" in query_params else None

        # Validate audit_id
        if audit_id is None:
            raise ValidationError("audit_id is required.")
        if clean is not None:
            try:
                clean = bool(strtobool(clean))
            except ValueError:
                clean = None

        try:
            audit = AuditProcessor.objects.get(id=audit_id)
        except Exception as e:
            raise ValidationError("Audit with id {} does not exist.".format(audit_id))

        a = AuditExporter.objects.filter(
            audit=audit,
            clean=clean,
            final=True
        )
        if a.count() == 0:
            try:
                a = AuditExporter.objects.get(
                        audit=audit,
                        clean=clean,
                        completed__isnull=True
                )
                return Response({
                    'message': 'export still pending.',
                    'id': a.id
                })
            except AuditExporter.DoesNotExist:
                a = AuditExporter.objects.create(
                    audit=audit,
                    clean=clean,
                    owner=request.user
                )
                return Response({
                    'message': 'Processing.  You will receive an email when your export is ready.',
                    'id': a.id,
                })
        else:
            a = a[0]
            if a.completed and a.file_name:
                file_url = AuditS3Exporter.generate_temporary_url(a.file_name, 604800)
                return Response({
                    'export_url': file_url,
                })
            else:
                return Response({
                    'message': 'export still pending.',
                    'id': a.id
                })

    def get_categories(self):
        categories = AuditCategory.objects.filter(category_display__isnull=True).values_list('category', flat=True)
        url = self.CATEGORY_API_URL.format(key=self.DATA_API_KEY, id=','.join(categories))
        r = requests.get(url)
        data = r.json()
        for i in data['items']:
            AuditCategory.objects.filter(category=i['id']).update(category_display=i['snippet']['title'])

    def export_videos(self, audit, audit_id=None, clean=None):
        clean_string = 'none'
        if clean is not None:
            clean_string = 'true' if clean else 'false'
        try:
            name = audit.params['name'].replace("/", "-")
        except Exception as e:
            name = audit_id
        file_name = 'export_{}_{}_{}.csv'.format(audit_id, name, clean_string)
        # If audit already exported, simply generate and return temp link
        if 'export_{}'.format(clean_string) in audit.params:
            return audit.params['export_{}'.format(clean_string)], file_name
        self.get_categories()
        cols = [
            "Video ID",
            "Name",
            "Language",
            "Category",
            "Views",
            "Likes",
            "Dislikes",
            "Emoji",
            "Publish Date",
            "Channel Name",
            "Channel ID",
            "Channel Default Lang.",
            "Channel Subscribers",
            "Country",
            "All Hit Words",
            "Unique Hit Words",
            "Video Count",
        ]
        video_ids = []
        hit_words = {}
        videos = AuditVideoProcessor.objects.filter(audit_id=audit_id)
        if clean is not None:
            videos = videos.filter(clean=clean)
        videos = videos.select_related("video")
        for vid in videos:
            video_ids.append(vid.video_id)
            hit_words[vid.video.video_id] = vid.word_hits
        video_meta = AuditVideoMeta.objects.filter(video_id__in=video_ids)
        with open(file_name, 'a+', newline='') as myfile:
            wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
            wr.writerow(cols)
            for v in video_meta:
                try:
                    language = v.language.language
                except Exception as e:
                    language = ""
                try:
                    category = v.category.category_display
                except Exception as e:
                    category = ""
                try:
                    country = v.video.channel.auditchannelmeta.country.country
                except Exception as e:
                    country = ""
                try:
                    channel_lang = v.video.channel.auditchannelmeta.language.language
                except Exception as e:
                    channel_lang = ""
                try:
                    video_count = v.video.channel.auditchannelmeta.video_count
                except Exception as e:
                    video_count = ""
                all_hit_words, unique_hit_words = self.get_hit_words(hit_words, v.video.video_id, clean=clean)
                data = [
                    v.video.video_id,
                    v.name,
                    language,
                    category,
                    v.views,
                    v.likes,
                    v.dislikes,
                    'T' if v.emoji else 'F',
                    v.publish_date.strftime("%m/%d/%Y") if v.publish_date else "",
                    v.video.channel.auditchannelmeta.name if v.video.channel else "",
                    v.video.channel.channel_id if v.video.channel else "",
                    channel_lang,
                    v.video.channel.auditchannelmeta.subscribers if v.video.channel else "",
                    country,
                    all_hit_words,
                    unique_hit_words,
                    video_count if video_count else "",
                ]
                wr.writerow(data)
            myfile.buffer.seek(0)

        with open(file_name) as myfile:
            s3_file_name = uuid4().hex
            download_file_name = file_name
            AuditS3Exporter.export_to_s3(myfile.buffer.raw, s3_file_name, download_file_name)
            if audit and audit.completed:
                audit.params['export_{}'.format(clean_string)] = s3_file_name
                audit.save()
            os.remove(myfile.name)
        return s3_file_name, download_file_name

    def export_channels(self, audit, audit_id=None, clean=None):
        if not audit_id:
            audit_id = audit.id
        clean_string = 'none'
        if clean is not None:
            clean_string = 'true' if clean else 'false'
        try:
            name = audit.params['name'].replace("/", "-")
        except Exception as e:
            name = audit_id
        file_name = 'export_{}_{}_{}.csv'.format(audit_id, name, clean_string)
        # If audit already exported, simply generate and return temp link
        if 'export_{}'.format(clean_string) in audit.params:
            return audit.params['export_{}'.format(clean_string)], file_name
        self.get_categories()
        cols = [
            "Channel Title",
            "Channel ID",
            "Views",
            "Subscribers",
            "Num Videos Checked",
            "Country",
            "Language",
            "Num Bad Videos",
            "Unique Bad Words",
            "Bad Words",
        ]
        channel_ids = []
        hit_words = {}
        video_count = {}
        channels = AuditChannelProcessor.objects.filter(audit_id=audit_id)
        if clean is not None:
            channels = channels.filter(clean=clean)
        # channels = channels.select_related("channel")
        bad_videos_count = {}
        for cid in channels:
            channel_ids.append(cid.channel_id)
            hit_words[cid.channel.channel_id] = cid.word_hits.get('exclusion')
            if not hit_words[cid.channel.channel_id]:
                hit_words[cid.channel.channel_id] = []
            videos = AuditVideoProcessor.objects.filter(audit_id=audit_id, video__channel_id=cid.channel_id)
            video_count[cid.channel.channel_id] = videos.count()
            bad_videos_count[cid.channel.channel_id] = 0
            for video in videos.filter(clean=False):
                bad_videos_count[cid.channel.channel_id] +=1
                if video.word_hits.get('exclusion'):
                    for bad_word in video.word_hits.get('exclusion'):
                        if bad_word not in hit_words[cid.channel.channel_id]:
                            hit_words[cid.channel.channel_id].append(bad_word)
        channel_meta = AuditChannelMeta.objects.filter(channel_id__in=channel_ids)
        with open(file_name, 'a+', newline='') as myfile:
            wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
            wr.writerow(cols)
            for v in channel_meta:
                try:
                    language = v.language.language
                except Exception as e:
                    language = ""
                try:
                    country = v.country.country
                except Exception as e:
                    country = ""
                data = [
                    v.name,
                    v.channel.channel_id,
                    v.view_count if v.view_count else "",
                    v.subscribers,
                    video_count[v.channel.channel_id],
                    country,
                    language,
                    bad_videos_count[v.channel.channel_id],
                    len(hit_words[v.channel.channel_id]),
                    ','.join(hit_words[v.channel.channel_id])
                ]
                wr.writerow(data)
            myfile.buffer.seek(0)

        with open(file_name) as myfile:
            s3_file_name = uuid4().hex
            download_file_name = file_name
            AuditS3Exporter.export_to_s3(myfile.buffer.raw, s3_file_name, download_file_name)
            os.remove(myfile.name)
            if audit and audit.completed:
                audit.params['export_{}'.format(clean_string)] = s3_file_name
                audit.save()
        return s3_file_name, download_file_name

    def get_hit_words(self, hit_words, v_id, clean=None):
        hits = hit_words.get(v_id)
        uniques = []
        words_to_use = 'exclusion'
        if clean is None or clean=='True':
            words_to_use = 'inclusion'
        if hits:
            if hits.get(words_to_use):
                for word in hits[words_to_use]:
                    if word not in uniques:
                        uniques.append(word)
                return len(hits[words_to_use]), ','.join(uniques)
        return "", ""

    def put_file_on_s3_and_create_url(self, file, s3_name, download_name):
        AuditS3Exporter.export_to_s3(file, s3_name, download_name)
        url = AuditS3Exporter.generate_temporary_url(s3_name)
        return url


class AuditS3Exporter(S3Exporter):
    bucket_name = settings.AMAZON_S3_AUDITS_EXPORTS_BUCKET_NAME
    export_content_type = "application/CSV"

    @classmethod
    def _presigned_s3(cls):
        s3 = boto3.client(
            "s3",
            aws_access_key_id=cls.aws_access_key_id,
            aws_secret_access_key=cls.aws_secret_access_key,
            config=Config(signature_version='s3v4')
        )
        return s3

    @classmethod
    def get_s3_key(cls, name):
        key = name
        return key

    @classmethod
    def export_to_s3(cls, exported_file, s3_name, download_name=None):
        if download_name is None:
            download_name = s3_name + ".csv"
        cls._s3().put_object(
            Bucket=cls.bucket_name,
            Key=cls.get_s3_key(s3_name),
            Body=exported_file,
            ContentDisposition='attachment; filename="{}"'.format(download_name),
            ContentType="text/csv"
        )

    @classmethod
    def generate_temporary_url(cls, key_name, time_limit=3600):
        return cls._presigned_s3().generate_presigned_url(
            ClientMethod="get_object",
            Params={
                "Bucket": cls.bucket_name,
                "Key": key_name
            },
            ExpiresIn=time_limit
        )
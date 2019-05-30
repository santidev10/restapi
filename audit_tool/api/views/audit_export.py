from distutils.util import strtobool
import csv
import requests
import os

from audit_tool.models import AuditCategory
from audit_tool.models import AuditVideoProcessor
from audit_tool.models import AuditVideoMeta
from audit_tool.models import AuditChannelProcessor
from audit_tool.models import AuditChannelMeta
from audit_tool.models import AuditProcessor

from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.exceptions import ValidationError
from rest_framework.status import HTTP_200_OK

from django.http import StreamingHttpResponse
from django.http import Http404
from django.conf import settings
from utils.aws.s3_exporter import S3Exporter


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
                clean = strtobool(clean)
            except ValidationError:
                raise ValidationError("Expected clean var to have boolean value or no value. Received {}.".format(clean))

        try:
            audit = AuditProcessor.objects.get(id=audit_id)
        except Exception as e:
            raise ValidationError("Audit with id {} does not exist.".format(audit_id))

        audit_type = audit.audit_type
        if audit_type == 2:
            file_name = self.export_channels(audit=audit, audit_id=audit_id, clean=clean)
        else:
            file_name = self.export_videos(audit=audit, audit_id=audit_id, clean=clean)

        try:
            content_generator = AuditS3Exporter.get_s3_export_content(file_name).iter_chunks()
        except Exception as e:
            raise ValidationError("File with name: {} not found on S3.".format(file_name))

        response = StreamingHttpResponse(
            content_generator,
            content_type="application/CSV",
            status=HTTP_200_OK,
        )
        response["Content-Disposition"] = "attachment; filename={}".format(file_name)
        return response

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
        # If audit already exported, simply generate and return temp link
        if 'export_{}'.format(clean_string) in audit.params:
            file_name = 'export_{}_{}_{}.csv'.format(audit_id, name, clean_string)
            return file_name
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
        videos = AuditVideoProcessor.objects.filter(audit_id=audit_id, clean=clean).select_related("video")#.values_list('video_id', flat=True)
        for vid in videos:
            video_ids.append(vid.video_id)
            hit_words[vid.video.video_id] = vid.word_hits
        video_meta = AuditVideoMeta.objects.filter(video_id__in=video_ids).select_related(
                "video",
                "video__channel",
                "video__channel__auditchannelmeta",
                "video__channel__auditchannelmeta__country",
                "language",
                "category"
        )
        with open('export_{}_{}.csv'.format(name, clean_string), 'w+', newline='') as myfile:
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
                    channel_lang = ''
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
                    v.publish_date.strftime("%m/%d/%Y, %H:%M:%S") if v.publish_date else '',
                    v.video.channel.auditchannelmeta.name if v.video.channel else '',
                    v.video.channel.channel_id if v.video.channel else '',
                    channel_lang,
                    v.video.channel.auditchannelmeta.subscribers if v.video.channel else '',
                    country,
                    all_hit_words,
                    unique_hit_words,
                    v.video.channel.auditchannelmeta.video_count if v.video.channel else '',
                ]
                wr.writerow(data)
            file_name = 'export_{}_{}_{}.csv'.format(audit_id, name, clean_string)
            myfile.buffer.seek(0)
            url = self.put_file_on_s3_and_create_url(myfile.buffer.raw, file_name)
            os.remove(myfile.name)
            if audit and audit.completed:
                audit.params['export_{}'.format(clean_string)] = file_name
                audit.save()
        return file_name

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
        # If audit already exported, simply generate and return temp link
        if 'export_{}'.format(clean_string) in audit.params:
            file_name = 'export_{}_{}_{}.csv'.format(audit_id, name, clean_string)
            return file_name
        self.get_categories()
        cols = [
            "Channel Title",
            "Channel ID",
            "Views",
            "Subscribers",
            "Num Videos Checked",
            "Country",
            "Language",
            "Unique Bad Words",
            "Bad Words",
        ]
        channel_ids = []
        hit_words = {}
        video_count = {}
        channels = AuditChannelProcessor.objects.filter(audit_id=audit_id).select_related("channel")
        for cid in channels:
            channel_ids.append(cid.channel_id)
            hit_words[cid.channel.channel_id] = cid.word_hits.get('exclusion')
            if not hit_words[cid.channel.channel_id]:
                hit_words[cid.channel.channel_id] = []
            videos = AuditVideoProcessor.objects.filter(audit_id=audit_id, video__channel_id=cid.channel_id)
            video_count[cid.channel.channel_id] = videos.count()
            for video in videos:
                if video.word_hits.get('exclusion'):
                    for bad_word in video.word_hits.get('exclusion'):
                        if bad_word not in hit_words[cid.channel.channel_id]:
                            hit_words[cid.channel.channel_id].append(bad_word)
        channel_meta = AuditChannelMeta.objects.filter(channel_id__in=channel_ids).select_related(
                "channel",
                "language",
                "country"
        )
        with open('export_{}.csv'.format(name), 'w+', newline='') as myfile:
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
                    v.view_count,
                    v.subscribers,
                    video_count[v.channel.channel_id],
                    country,
                    language,
                    len(hit_words[v.channel.channel_id]),
                    ','.join(hit_words[v.channel.channel_id])
                ]
                wr.writerow(data)
            file_name = 'export_{}_{}_{}.csv'.format(audit_id, name, clean_string)
            myfile.buffer.seek(0)
            url = self.put_file_on_s3_and_create_url(myfile.buffer.raw, file_name)
            os.remove(myfile.name)
            if audit and audit.completed:
                audit.params['export_{}'.format(clean_string)] = file_name
                audit.save()
        return file_name

    @staticmethod
    def get_hit_words(hit_words, v_id, clean=None):
        hits = hit_words.get(v_id)
        uniques = []
        words_to_use = 'exclusion'
        if clean and clean=='True':
            words_to_use = 'inclusion'
        if hits:
            if hits.get(words_to_use):
                for word in hits[words_to_use]:
                    if word not in uniques:
                        uniques.append(word)
                return len(hits[words_to_use]), ','.join(uniques)
        return '', ''

    @staticmethod
    def put_file_on_s3_and_create_url(file, name):
        AuditS3Exporter.export_to_s3(file, name)
        url = AuditS3Exporter.generate_temporary_url(name)
        return url


class AuditS3Exporter(S3Exporter):
    bucket_name = settings.AMAZON_S3_AUDITS_EXPORTS_BUCKET_NAME
    export_content_type = "application/CSV"

    @classmethod
    def get_s3_key(cls, name):
        key = name
        return key

    @classmethod
    def export_to_s3(cls, exported_file, name):
        cls._s3().put_object(
            Bucket=cls.bucket_name,
            Key=cls.get_s3_key(name),
            Body=exported_file
        )

    @classmethod
    def generate_temporary_url(cls, key_name, time_limit=3600):
        return cls._s3().generate_presigned_url(
            ClientMethod="get_object",
            Params={
                "Bucket": cls.bucket_name,
                "Key": key_name
            },
            ExpiresIn=time_limit
        )

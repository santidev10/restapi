from distutils.util import strtobool
import csv
import requests
import os
from uuid import uuid4
from datetime import timedelta

from audit_tool.models import AuditCategory
from audit_tool.models import AuditExporter
from audit_tool.models import AuditVideoProcessor
from audit_tool.models import AuditVideoMeta
from audit_tool.models import AuditChannelProcessor
from audit_tool.models import AuditChannelMeta
from audit_tool.models import AuditProcessor
from brand_safety.auditors.brand_safety_audit import BrandSafetyAudit

from rest_framework.views import APIView
from rest_framework.exceptions import ValidationError

from rest_framework.response import Response
from django.conf import settings
from utils.aws.s3_exporter import S3Exporter
import boto3
from botocore.client import Config
from utils.permissions import user_has_permission


class AuditExportApiView(APIView):
    permission_classes = (
        user_has_permission("userprofile.view_audit"),
    )

    CATEGORY_API_URL = "https://www.googleapis.com/youtube/v3/videoCategories" \
                       "?key={key}&part=id,snippet&id={id}"
    DATA_API_KEY = settings.YOUTUBE_API_DEVELOPER_KEY
    MAX_ROWS = 750000

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
                    owner_id=request.user.id
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

    def clean_duration(self, duration):
        try:
            delimiters = ["W", "D", "H", "M", "S"]
            duration = duration.replace("P", "").replace("T", "")
            current_num = ""
            time_duration = timedelta(0)
            for char in duration:
                if char in delimiters:
                    if char == "W":
                        time_duration += timedelta(weeks=int(current_num))
                    elif char == "D":
                        time_duration += timedelta(days=int(current_num))
                    elif char == "H":
                        time_duration += timedelta(hours=int(current_num))
                    elif char == "M":
                        time_duration += timedelta(minutes=int(current_num))
                    elif char == "S":
                        time_duration += timedelta(seconds=int(current_num))
                    current_num = ""
                else:
                    current_num += char
            if time_duration.days > 0:
                seconds = time_duration.seconds
                days = time_duration.days
                hours = seconds // 3600
                seconds -= (hours * 3600)
                minutes = seconds // 60
                seconds -= (minutes * 60)
                hours += (days * 24)
                time_string = "{:02}:{:02}:{:02}".format(int(hours), int(minutes), int(seconds))
            else:
                time_string = str(time_duration)
            return time_string
        except Exception as e:
            return ""

    def export_videos(self, audit, audit_id=None, clean=None, export=None):
        clean_string = 'none'
        if clean is not None:
            clean_string = 'true' if clean else 'false'
        try:
            name = audit.params['name'].replace("/", "-")
        except Exception as e:
            name = audit_id
        file_name = 'export_{}_{}_{}.csv'.format(audit_id, name, clean_string)
        exports = AuditExporter.objects.filter(
            audit=audit,
            clean=clean,
            final=True
        )
        if exports.count() > 0:
            return exports[0].file_name, _
        # if 'export_{}'.format(clean_string) in audit.params:
        #     return audit.params['export_{}'.format(clean_string)], file_name
        self.get_categories()
        do_hit_words = False
        if clean is False:
            hit_types = 'exclusion'
            if self.audit.params.get('exclusion'):
                do_hit_words = True
        else:
            hit_types = 'inclusion'
            if self.audit.params.get('inclusion'):
                do_hit_words = True
        cols = [
            "Video URL",
            "Name",
            "Language",
            "Category",
            "Views",
            "Likes",
            "Dislikes",
            "Emoji",
            "Default Audio Language",
            "Duration",
            "Publish Date",
            "Channel Name",
            "Channel ID",
            "Channel Default Lang.",
            "Channel Subscribers",
            "Country",
            "Last Uploaded Video",
            "Last Uploaded Video Views",
            "Last Uploaded Category",
            "All {} Hit Words".format(hit_types),
            "Unique {} Hit Words".format(hit_types),
            "Video Count",
            "Brand Safety Score",
        ]
        if clean is False:
            try:
                bad_word_categories = set(audit.params['exclusion_category'])
                if "" in bad_word_categories:
                    bad_word_categories.remove("")
                if len(bad_word_categories) > 0:
                    cols.extend(bad_word_categories)
            except Exception as e:
                pass
        video_ids = []
        hit_words = {}
        videos = AuditVideoProcessor.objects.filter(audit_id=audit_id)
        if clean is not None:
            videos = videos.filter(clean=clean)
        videos = videos.select_related("video")
        for vid in videos:
            video_ids.append(vid.video_id)
            if do_hit_words:
                hit_words[vid.video.video_id] = vid.word_hits
        video_meta = AuditVideoMeta.objects.filter(video_id__in=video_ids)
        auditor = BrandSafetyAudit(discovery=False)
        with open(file_name, 'w+', newline='') as myfile:
            wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
            wr.writerow(cols)
            count = video_meta.count()
            if count > self.MAX_ROWS:
                count = self.MAX_ROWS
            num_done = 0
            for v in video_meta:
                if num_done > self.MAX_ROWS:
                    continue
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
                try:
                    last_uploaded = v.video.channel.auditchannelmeta.last_uploaded.strftime("%m/%d/%Y")
                except Exception as e:
                    last_uploaded = ""
                try:
                    last_uploaded_view_count = v.video.channel.auditchannelmeta.last_uploaded_view_count
                except Exception as e:
                    last_uploaded_view_count = ''
                try:
                    last_uploaded_category = v.video.channel.auditchannelmeta.last_uploaded_category.category_display
                except Exception as e:
                    last_uploaded_category = ''
                try:
                    default_audio_language = v.default_audio_language.language
                except Exception as e:
                    default_audio_language = ""
                all_hit_words = ""
                unique_hit_words = ""
                if do_hit_words:
                    all_hit_words, unique_hit_words = self.get_hit_words(hit_words, v.video.video_id, clean=clean)
                video_audit_score = auditor.audit_video({
                    "id": v.video.video_id,
                    "title": v.name,
                    "description": v.description,
                    "tags": v.keywords,
                }, full_audit=False)
                data = [
                    "https://www.youtube.com/video/" + v.video.video_id,
                    v.name,
                    language,
                    category,
                    v.views,
                    v.likes,
                    v.dislikes,
                    'T' if v.emoji else 'F',
                    default_audio_language,
                    self.clean_duration(v.duration) if v.duration else "",
                    v.publish_date.strftime("%m/%d/%Y") if v.publish_date else "",
                    v.video.channel.auditchannelmeta.name if v.video.channel else "",
                    v.video.channel.channel_id if v.video.channel else "",
                    channel_lang,
                    v.video.channel.auditchannelmeta.subscribers if v.video.channel else "",
                    country,
                    last_uploaded,
                    last_uploaded_view_count,
                    last_uploaded_category,
                    all_hit_words,
                    unique_hit_words,
                    video_count if video_count else "",
                    video_audit_score,
                ]
                if clean is False:
                    try:
                        if len(bad_word_categories) > 0:
                            bad_word_category_dict = {}
                            bad_words = unique_hit_words.split(",")
                            for word in bad_words:
                                try:
                                    word_index = audit.params['exclusion'].index(word)
                                    category = audit.params['exclusion_category'][word_index]
                                    if category in bad_word_category_dict:
                                        bad_word_category_dict[category].append(word)
                                    else:
                                        bad_word_category_dict[category] = [word]
                                except Exception as e:
                                    pass
                            for category in bad_word_categories:
                                if category in bad_word_category_dict:
                                    data.append(len(bad_word_category_dict[category]))
                                else:
                                    data.append(0)
                    except Exception as e:
                        pass
                wr.writerow(data)
                num_done += 1
                if export and num_done % 500 == 0:
                    export.percent_done = int(1.0 * num_done / count * 100)
                    export.save(update_fields=['percent_done'])
                    print("export at {}".format(export.percent_done))

        with open(file_name) as myfile:
            s3_file_name = uuid4().hex
            download_file_name = file_name
            AuditS3Exporter.export_to_s3(myfile.buffer.raw, s3_file_name, download_file_name)
            if audit and audit.completed:
                audit.params['export_{}'.format(clean_string)] = s3_file_name
                audit.save()
            os.remove(myfile.name)
        return s3_file_name, download_file_name

    def export_channels(self, audit, audit_id=None, clean=None, export=None):
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
        exports = AuditExporter.objects.filter(
            audit=audit,
            clean=clean,
            final=True
        )
        if exports.count() > 0:
            return exports[0].file_name, None
        self.get_categories()
        cols = [
            "Channel Title",
            "Channel URL",
            "Views",
            "Subscribers",
            "Num Videos Checked",
            "Num Videos Total",
            "Country",
            "Language",
            "Last Video Upload",
            "Last Video Views",
            "Last Video Category",
            "Num Bad Videos",
            "Unique {} Words".format("Bad" if clean is False else "Good"),
            "{} Words".format("Bad" if clean is False else "Good"),
            "Brand Safety Score",
        ]
        if clean is None:
            cols.insert(-1, "Unique Bad Words")
            cols.insert(-1, "Bad Words")
        try:
            bad_word_categories = set(audit.params['exclusion_category'])
            if "" in bad_word_categories:
                bad_word_categories.remove("")
            if len(bad_word_categories) > 0:
                cols.extend(bad_word_categories)
        except Exception as e:
            pass
        channel_ids = []
        hit_words = {}
        good_hit_words = {}
        bad_hit_words = {}
        video_count = {}
        channels = AuditChannelProcessor.objects.filter(audit_id=audit_id)
        if clean is not None:
            channels = channels.filter(clean=clean)
        bad_videos_count = {}
        for cid in channels:
            channel_ids.append(cid.channel_id)
            node = ''
            try:
                if clean is False:
                    hit_words[cid.channel.channel_id] = set(cid.word_hits.get('exclusion'))
                    node = 'exclusion'
                elif clean is True:
                    hit_words[cid.channel.channel_id] = set(cid.word_hits.get('inclusion'))
                    node = 'inclusion'
                elif clean is None:
                    good_hit_words[cid.channel.channel_id] = set(cid.word_hits.get('inclusion'))
                    bad_hit_words[cid.channel.channel_id] = set(cid.word_hits.get('exclusion'))
                    node = 'all'
            except Exception as e:
                hit_words[cid.channel.channel_id] = set()
                good_hit_words[cid.channel.channel_id] = set()
                bad_hit_words[cid.channel.channel_id] = set()
            videos = AuditVideoProcessor.objects.filter(
                audit_id=audit_id,
                video__channel_id=cid.channel_id
            )
            video_count[cid.channel.channel_id] = videos.count()
            bad_videos_count[cid.channel.channel_id] = videos.filter(clean=False).count()
            if node == 'all':
                for video in videos.filter(clean=True):
                    if video.word_hits.get('inclusion'):
                        good_hit_words[cid.channel.channel_id] = \
                            good_hit_words[cid.channel.channel_id].union(set(video.word_hits.get('inclusion')))
                for video in videos.filter(clean=False):
                    if video.word_hits.get('exclusion'):
                        bad_hit_words[cid.channel.channel_id] = \
                            bad_hit_words[cid.channel.channel_id].union(set(video.word_hits.get('exclusion')))
            else:
                videos_filter = False if clean is False else True
                for video in videos.filter(clean=videos_filter):
                    if video.word_hits.get(node):
                        hit_words[cid.channel.channel_id] = \
                            hit_words[cid.channel.channel_id].union(set(video.word_hits.get(node)))
        channel_meta = AuditChannelMeta.objects.filter(channel_id__in=channel_ids)
        auditor = BrandSafetyAudit(discovery=False)
        with open(file_name, 'w+', newline='') as myfile:
            wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
            wr.writerow(cols)
            count = channel_meta.count()
            num_done = 0
            for v in channel_meta:
                try:
                    language = v.language.language
                except Exception as e:
                    language = ""
                try:
                    country = v.country.country
                except Exception as e:
                    country = ""
                try:
                    last_category = v.last_uploaded_category.category_display
                except Exception as e:
                    last_category = ""
                channel_brand_safety_score = auditor.audit_channel(v.channel.channel_id, rescore=False)
                data = [
                    v.name,
                    "https://www.youtube.com/channel/" + v.channel.channel_id,
                    v.view_count if v.view_count else "",
                    v.subscribers,
                    video_count[v.channel.channel_id],
                    v.video_count,
                    country,
                    language,
                    v.last_uploaded.strftime("%Y/%m/%d") if v.last_uploaded else '',
                    v.last_uploaded_view_count if v.last_uploaded_view_count else '',
                    last_category,
                    bad_videos_count[v.channel.channel_id],
                    len(hit_words[v.channel.channel_id]) if clean is not None else len(good_hit_words[v.channel.channel_id]),
                    ','.join(hit_words[v.channel.channel_id]) if clean is not None else ','.join(good_hit_words[v.channel.channel_id]),
                    channel_brand_safety_score
                ]
                if clean is None:
                    data.insert(-1, len(bad_hit_words[v.channel.channel_id]))
                    data.insert(-1, ','.join(bad_hit_words[v.channel.channel_id]))
                try:
                    if len(bad_word_categories) > 0:
                        bad_word_category_dict = {}
                        bad_words = hit_words[v.channel.channel_id] if clean is None else bad_hit_words[v.channel.channel_id]
                        for word in bad_words:
                            try:
                                word_index = audit.params['exclusion'].index(word)
                                category = audit.params['exclusion_category'][word_index]
                                if category in bad_word_category_dict:
                                    bad_word_category_dict[category].append(word)
                                else:
                                    bad_word_category_dict[category] = [word]
                            except Exception as e:
                                pass
                        for category in bad_word_categories:
                            if category in bad_word_category_dict:
                                data.append(len(bad_word_category_dict[category]))
                            else:
                                data.append(0)
                except Exception as e:
                    pass
                wr.writerow(data)
                num_done += 1
                if export and num_done % 500 == 0:
                    export.percent_done = int(num_done / count * 100.0)
                    export.save(update_fields=['percent_done'])
                    print("export at {}".format(export.percent_done))

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
        uniques = set()
        words_to_use = 'exclusion'
        if clean is None or clean==True:
            words_to_use = 'inclusion'
        if hits:
            if hits.get(words_to_use):
                for word in hits[words_to_use]:
                    uniques.add(word)
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

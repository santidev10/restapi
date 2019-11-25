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
from utils.brand_safety import map_brand_safety_score


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
        export_as_videos = bool(strtobool(query_params["export_as_videos"])) if "export_as_videos" in query_params else False

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
            final=True,
            export_as_videos=export_as_videos
        )
        if a.count() == 0:
            try:
                a = AuditExporter.objects.get(
                    audit=audit,
                    clean=clean,
                    completed__isnull=True,
                    export_as_videos=export_as_videos,
                )
                return Response({
                    'message': 'export still pending.',
                    'id': a.id
                })
            except AuditExporter.DoesNotExist:
                a = AuditExporter.objects.create(
                    audit=audit,
                    clean=clean,
                    owner_id=request.user.id,
                    export_as_videos=export_as_videos
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
        file_name = 'export_{}_{}_{}_{}.csv'.format(audit_id, name, clean_string, str(export.export_as_videos))
        exports = AuditExporter.objects.filter(
            audit=audit,
            clean=clean,
            final=True,
            export_as_videos=export.export_as_videos
        )
        if exports.count() > 0:
            return exports[0].file_name, _
        self.get_categories()
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
            "All Good Hit Words",
            "Unique Good Hit Words",
            "All Bad Hit Words",
            "Unique Bad Hit Words",
            "Video Count",
            "Brand Safety Score",
        ]
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
        for vid in videos:
            video_ids.append(vid.video_id)
            hit_words[vid.video.video_id] = vid.word_hits
        video_meta = AuditVideoMeta.objects.filter(video_id__in=video_ids)
        auditor = BrandSafetyAudit(discovery=False)
        rows = [cols]
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
            all_good_hit_words, unique_good_hit_words = self.get_hit_words(hit_words, v.video.video_id, clean=True)
            all_bad_hit_words, unique_bad_hit_words = self.get_hit_words(hit_words, v.video.video_id, clean=False)
            video_audit_score = auditor.audit_video({
                "id": v.video.video_id,
                "title": v.name,
                "description": v.description,
                "tags": v.keywords,
            }, full_audit=False)
            mapped_score = map_brand_safety_score(video_audit_score)
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
                "https://www.youtube.com/channel/" + v.video.channel.channel_id if v.video.channel else "",
                channel_lang,
                v.video.channel.auditchannelmeta.subscribers if v.video.channel else "",
                country,
                last_uploaded,
                last_uploaded_view_count,
                last_uploaded_category,
                all_good_hit_words,
                unique_good_hit_words,
                all_bad_hit_words,
                unique_bad_hit_words,
                video_count if video_count else "",
                mapped_score,
            ]
            try:
                if len(bad_word_categories) > 0:
                    bad_word_category_dict = {}
                    bad_words = unique_bad_hit_words.split(",")
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
            rows.append(data)
            num_done += 1
            if export and num_done % 500 == 0:
                export.percent_done = int(1.0 * num_done / count * 100) - 5
                if export.percent_done < 0:
                    export.percent_done = 0
                export.save(update_fields=['percent_done'])
                print("export at {}".format(export.percent_done))
        with open(file_name, 'w+', newline='') as myfile:
            wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
            for row in rows:
                wr.writerow(row)

        with open(file_name) as myfile:
            s3_file_name = uuid4().hex
            download_file_name = file_name
            AuditS3Exporter.export_to_s3(myfile.buffer.raw, s3_file_name, download_file_name)
            if audit and audit.completed:
                audit.params['export_{}'.format(clean_string)] = s3_file_name
                audit.save()
            os.remove(myfile.name)
        return s3_file_name, download_file_name

    def check_legacy(self, audit):
        empty_channel_avps = AuditVideoProcessor.objects.filter(audit=audit, channel__isnull=True)
        if empty_channel_avps.exists():
            for avp in empty_channel_avps:
                try:
                    avp.channel = avp.video.channel
                    avp.save(update_fields=['channel'])
                except Exception as e:
                    pass
            
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
            "Unique Exclusion Words (channel)",
            "Unique Exclusion Words (videos)",
            "Exclusion Words (channel)",
            "Exclusion Words (video)",
            "Inclusion Words (channel)",
            "Inclusion Words (video)",
            "Brand Safety Score",
        ]
        try:
            bad_word_categories = set(audit.params['exclusion_category'])
            if "" in bad_word_categories:
                bad_word_categories.remove("")
            if len(bad_word_categories) > 0:
                cols.extend(bad_word_categories)
        except Exception as e:
            pass
        channel_ids = []
        good_hit_words = {}
        bad_hit_words = {}
        bad_video_hit_words = {}
        good_video_hit_words = {}
        video_count = {}
        self.check_legacy(audit)
        channels = AuditChannelProcessor.objects.filter(audit_id=audit_id)
        if clean is not None:
            channels = channels.filter(clean=clean)
        bad_videos_count = {}
        for cid in channels:
            channel_ids.append(cid.channel_id)
            try:
                good_hit_words[cid.channel.channel_id] = set(cid.word_hits.get('inclusion'))
                good_video_hit_words[cid.channel.channel_id] = set(cid.word_hits.get('inclusion_videos'))
            except Exception as e:
                good_hit_words[cid.channel.channel_id] = set()
                good_video_hit_words[cid.channel.channel_id] = set()
            try:
                bad_hit_words[cid.channel.channel_id] = set(cid.word_hits.get('exclusion'))
                bad_video_hit_words[cid.channel.channel_id] = set(cid.word_hits.get('exclusion_videos'))
            except Exception as e:
                bad_hit_words[cid.channel.channel_id] = set()
                bad_video_hit_words[cid.channel.channel_id] = set()
            videos = AuditVideoProcessor.objects.filter(
                audit_id=audit_id,
                channel_id=cid.channel_id
            )
            video_count[cid.channel.channel_id] = videos.count()
            bad_videos_count[cid.channel.channel_id] = videos.filter(clean=False).count()
        channel_meta = AuditChannelMeta.objects.filter(channel_id__in=channel_ids)
        auditor = BrandSafetyAudit(discovery=False)
        rows = [cols]
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
            mapped_score = map_brand_safety_score(channel_brand_safety_score)
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
                len(bad_hit_words[v.channel.channel_id]),
                len(bad_video_hit_words[v.channel.channel_id]),
                ','.join(bad_hit_words[v.channel.channel_id]),
                ','.join(bad_video_hit_words[v.channel.channel_id]),
                ','.join(good_hit_words[v.channel.channel_id]),
                ','.join(good_video_hit_words[v.channel.channel_id]),
                mapped_score
            ]
            try:
                if len(bad_word_categories) > 0:
                    bad_word_category_dict = {}
                    bad_words = bad_hit_words[v.channel.channel_id].union(bad_video_hit_words[v.channel.channel_id])
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
            num_done += 1
            rows.append(data)
            if export and num_done % 500 == 0:
                export.percent_done = int(num_done / count * 100.0) - 5
                if export.percent_done < 0:
                    export.percent_done = 0
                export.save(update_fields=['percent_done'])
                print("export at {}".format(export.percent_done))
        with open(file_name, 'w+', newline='') as myfile:
            wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
            for row in rows:
                wr.writerow(row)

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

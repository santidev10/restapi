from distutils.util import strtobool
import csv
import requests
import os
from uuid import uuid4
from datetime import timedelta

from audit_tool.models import AuditCategory
from audit_tool.models import AuditCountry
from audit_tool.models import AuditExporter
from audit_tool.models import AuditLanguage
from audit_tool.models import AuditVideoProcessor
from audit_tool.models import AuditChannelProcessor
from audit_tool.models import AuditProcessor
from brand_safety.auditors.brand_safety_audit import BrandSafetyAudit
from es_components.managers import ChannelManager
from es_components.constants import Sections

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
    cache = {}

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

    def get_lang(self, obj_id):
        if 'language' not in self.cache:
            self.cache['language'] = {}
        if obj_id not in self.cache['language']:
            self.cache['language'][obj_id] = AuditLanguage.objects.get(id=obj_id).language
        return self.cache['language'][obj_id]

    def get_category(self, obj_id):
        if 'category' not in self.cache:
            self.cache['category'] = {}
        if obj_id not in self.cache['category']:
            self.cache['category'][obj_id] = AuditCategory.objects.get(id=obj_id).category_display_iab
        return self.cache['category'][obj_id]

    def get_country(self, obj_id):
        if 'country' not in self.cache:
            self.cache['country'] = {}
        if obj_id not in self.cache['country']:
            self.cache['country'][obj_id] = AuditCountry.objects.get(id=obj_id).country
        return self.cache['country'][obj_id]

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
            return exports[0].file_name, None
        self.get_categories()
        do_inclusion = False
        if audit.params.get('inclusion') and len(audit.params.get('inclusion')) > 0:
            do_inclusion = True
        do_exclusion = False
        if audit.params.get('exclusion') and len(audit.params.get('exclusion')) > 0:
            do_exclusion = True
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
            "Made For Kids",
        ]
        try:
            bad_word_categories = set(audit.params['exclusion_category'])
            if "" in bad_word_categories:
                bad_word_categories.remove("")
            if len(bad_word_categories) > 0:
                cols.extend(bad_word_categories)
        except Exception as e:
            pass
        videos = AuditVideoProcessor.objects.filter(audit_id=audit_id)
        if clean is not None:
            videos = videos.filter(clean=clean)
        auditor = BrandSafetyAudit()
        rows = [cols]
        count = videos.count()
        if count > self.MAX_ROWS:
            count = self.MAX_ROWS
        num_done = 0
        for avp in videos:
            vid = avp.video
            try:
                v = vid.auditvideometa
            except Exception as e:
                v = None
            v_channel = vid.channel
            acm = v_channel.auditchannelmeta if v_channel else None
            if num_done > self.MAX_ROWS:
                continue
            try:
                language = self.get_lang(v.language_id)
            except Exception as e:
                language = ""
            try:
                category = self.get_category(v.category_id)
            except Exception as e:
                category = ""
            try:
                country = self.get_country(acm.country_id)
            except Exception as e:
                country = ""
            try:
                channel_lang = self.get_lang(acm.language_id)
            except Exception as e:
                channel_lang = ""
            try:
                video_count = acm.video_count
            except Exception as e:
                video_count = ""
            try:
                last_uploaded = acm.last_uploaded.strftime("%m/%d/%Y")
            except Exception as e:
                last_uploaded = ""
            try:
                last_uploaded_view_count = acm.last_uploaded_view_count
            except Exception as e:
                last_uploaded_view_count = ''
            try:
                last_uploaded_category = self.get_category(acm.last_uploaded_category_id)
            except Exception as e:
                last_uploaded_category = ''
            try:
                default_audio_language = self.get_lang(v.default_audio_language_id)
            except Exception as e:
                default_audio_language = ""
            v_word_hits = avp.word_hits
            if do_inclusion:
                all_good_hit_words, unique_good_hit_words = self.get_hit_words(v_word_hits, clean=True)
            else:
                all_good_hit_words = ""
                unique_good_hit_words = ""
            if do_exclusion or (v_word_hits and v_word_hits.get('exclusion') and v_word_hits.get('exclusion')==['ytAgeRestricted']):
                all_bad_hit_words, unique_bad_hit_words = self.get_hit_words(v_word_hits, clean=False)
            else:
                all_bad_hit_words = ""
                unique_bad_hit_words = ""
            try:
                video_audit_score = auditor.audit_video({
                    "id": vid.video_id,
                    "title": v.name,
                    "description": v.description,
                    "tags": v.keywords,
                }, full_audit=False)
                mapped_score = map_brand_safety_score(video_audit_score)
            except Exception as e:
                mapped_score = ""
                print("Problem calculating video score")
            data = [
                "https://www.youtube.com/video/" + vid.video_id,
                v.name if v else "",
                language,
                category,
                v.views if v else "",
                v.likes if v else "",
                v.dislikes if v else "",
                'T' if v and v.emoji else 'F',
                default_audio_language,
                self.clean_duration(v.duration) if v and v.duration else "",
                v.publish_date.strftime("%m/%d/%Y") if v and v.publish_date else "",
                acm.name if acm else "",
                "https://www.youtube.com/channel/" + v_channel.channel_id if v_channel else "",
                channel_lang,
                acm.subscribers if acm else "",
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
                v.made_for_kids if v else "",
            ]
            try:
                if len(bad_word_categories) > 0:
                    bad_word_category_dict = {}
                    bad_words = unique_bad_hit_words.split(",")
                    for word in bad_words:
                        try:
                            for i in range(len(audit.params['exclusion'])):
                                if audit.params['exclusion'][i][0] == word:
                                    word_index = i
                                    break
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
                audit.save(update_fields=['params'])
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
        do_inclusion = False
        if audit.params.get('inclusion') and len(audit.params.get('inclusion')) > 0:
            do_inclusion = True
        do_exclusion = False
        if audit.params.get('exclusion') and len(audit.params.get('exclusion')) > 0:
            do_exclusion = True
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
            "Num Kids Videos",
            "Unique Exclusion Words (channel)",
            "Unique Exclusion Words (videos)",
            "Exclusion Words (channel)",
            "Exclusion Words (video)",
            "Inclusion Words (channel)",
            "Inclusion Words (video)",
            "Brand Safety Score",
            "Monetised",
        ]
        try:
            bad_word_categories = set(audit.params['exclusion_category'])
            if "" in bad_word_categories:
                bad_word_categories.remove("")
            if len(bad_word_categories) > 0:
                cols.extend(bad_word_categories)
        except Exception as e:
            pass
        good_hit_words = {}
        bad_hit_words = {}
        bad_video_hit_words = {}
        good_video_hit_words = {}
        bad_videos_count = {}
        kid_videos_count = {}
        video_count = {}
        self.check_legacy(audit)
        channels = AuditChannelProcessor.objects.filter(audit_id=audit_id)
        if clean is not None:
            channels = channels.filter(clean=clean)
        for cid in channels:
            full_channel_id = cid.channel.channel_id
            if audit.params.get('do_videos'):
                try:
                    video_count[full_channel_id] = len(cid.word_hits.get('processed_video_ids'))
                except Exception as e:
                    pass
            if do_inclusion:
                try:
                    i = cid.word_hits.get('inclusion')
                    if i:
                        good_hit_words[full_channel_id] = set(i)
                    i_v = cid.word_hits.get('inclusion_videos')
                    if i_v:
                        good_video_hit_words[full_channel_id] = set(i_v)
                except Exception as e:
                    pass
            try:
                kid_videos_count[full_channel_id] = len(cid.word_hits.get('made_for_kids'))
            except Exception as e:
                pass
            if do_exclusion:
                try:
                    bad_videos_count[full_channel_id] = len(cid.word_hits.get('bad_video_ids'))
                except Exception as e:
                    pass
                try:
                    e = cid.word_hits.get('exclusion')
                    if e:
                        bad_hit_words[full_channel_id] = set(e)
                    e_v = cid.word_hits.get('exclusion_videos')
                    if e_v:
                        bad_video_hit_words[full_channel_id] = set(e_v)
                except Exception as e:
                    pass
        auditor = BrandSafetyAudit()
        rows = [cols]
        count = channels.count()
        num_done = 0
        sections = (Sections.MONETIZATION,)
        channel_manager = ChannelManager(sections)
        for db_channel in channels:
            channel = db_channel.channel
            v = channel.auditchannelmeta
            try:
                language = self.get_lang(v.language_id)
            except Exception as e:
                language = ""
            try:
                country = self.get_country(v.country_id)
            except Exception as e:
                country = ""
            try:
                last_category = self.get_category(v.last_uploaded_category_id)
            except Exception as e:
                last_category = ""
            try:
                channel_brand_safety_score = auditor.audit_channel(channel.channel_id, rescore=False)
                mapped_score = map_brand_safety_score(channel_brand_safety_score)
            except Exception as e:
                print(str(e))
            if not v.monetised:
                try:
                    cid = channel.channel_id
                    cm_channel = channel_manager.get([cid])[0]
                    if 'monetization' in cm_channel and cm_channel.monetization.is_monetizable:
                        v.monetised = True
                        v.save(update_fields=['monetised'])
                except Exception as e:
                    pass
            data = [
                v.name,
                "https://www.youtube.com/channel/" + channel.channel_id,
                v.view_count if v.view_count else "",
                v.subscribers,
                video_count.get(channel.channel_id) if video_count.get(channel.channel_id) else 0,
                v.video_count,
                country,
                language,
                v.last_uploaded.strftime("%Y/%m/%d") if v.last_uploaded else "",
                v.last_uploaded_view_count if v.last_uploaded_view_count else "",
                last_category,
                bad_videos_count.get(channel.channel_id) if bad_videos_count.get(channel.channel_id) else 0,
                kid_videos_count.get(channel.channel_id) if kid_videos_count.get(channel.channel_id) else 0,
                len(bad_hit_words.get(channel.channel_id)) if bad_hit_words.get(channel.channel_id) else 0,
                len(bad_video_hit_words.get(channel.channel_id)) if bad_video_hit_words.get(
                    channel.channel_id) else 0,
                ','.join(bad_hit_words.get(channel.channel_id)) if bad_hit_words.get(channel.channel_id) else "",
                ','.join(bad_video_hit_words.get(channel.channel_id)) if bad_video_hit_words.get(
                    channel.channel_id) else "",
                ','.join(good_hit_words.get(channel.channel_id)) if good_hit_words.get(channel.channel_id) else "",
                ','.join(good_video_hit_words.get(channel.channel_id)) if good_video_hit_words.get(
                    channel.channel_id) else "",
                mapped_score if mapped_score else "",
                'true' if v.monetised else "",
            ]
            try:
                if len(bad_word_categories) > 0:
                    bad_word_category_dict = {}
                    bad_words = set()
                    if channel.channel_id in bad_hit_words:
                        bad_words = bad_words.union(bad_hit_words[channel.channel_id])
                    if channel.channel_id in bad_video_hit_words:
                        bad_words = bad_words.union(bad_video_hit_words[channel.channel_id])
                    for word in bad_words:
                        try:
                            for i in range(len(audit.params['exclusion'])):
                                if audit.params['exclusion'][i][0] == word:
                                    word_index = i
                                    break
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
            if export and num_done % 250 == 0:
                old_percent = export.percent_done
                export.percent_done = int(num_done / count * 100.0) - 5
                if export.percent_done < 0:
                    export.percent_done = 0
                if export.percent_done > old_percent:
                    export.save(update_fields=['percent_done'])
                print("export at {}, {}/{}".format(export.percent_done, num_done, count))
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
                audit.save(update_fields=['params'])
        return s3_file_name, download_file_name

    def get_hit_words(self, hits, clean=None):
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

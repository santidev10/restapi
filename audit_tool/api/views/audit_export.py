import csv
import os
from collections import defaultdict
from datetime import timedelta
from distutils.util import strtobool
from uuid import uuid4

import boto3
import requests
import unicodedata
from botocore.client import Config
from django.conf import settings
from rest_framework.exceptions import ValidationError
from rest_framework.response import Response
from rest_framework.views import APIView

from audit_tool.models import AuditCategory
from audit_tool.models import AuditChannelProcessor
from audit_tool.models import AuditCountry
from audit_tool.models import AuditExporter
from audit_tool.models import AuditLanguage
from audit_tool.models import AuditProcessor
from audit_tool.models import AuditVideoMeta
from audit_tool.models import AuditVideoProcessor
from audit_tool.models import BlacklistItem
from brand_safety.auditors.video_auditor import VideoAuditor
from es_components.constants import Sections
from es_components.managers import ChannelManager
from userprofile.constants import StaticPermissions
from utils.aws.s3_exporter import S3Exporter
from utils.brand_safety import map_brand_safety_score
from utils.permissions import user_has_permission
from utils.utils import chunks_generator


class AuditExportApiView(APIView):
    permission_classes = (
        StaticPermissions.has_perms(StaticPermissions.AUDIT_QUEUE__READ),
    )

    CATEGORY_API_URL = "https://www.googleapis.com/youtube/v3/videoCategories" \
                       "?key={key}&part=id,snippet&id={id}"
    DATA_API_KEY = settings.YOUTUBE_API_DEVELOPER_KEY
    MAX_ROWS = 1000000
    cache = {}
    local_file = None

    def get(self, request):
        query_params = request.query_params
        audit_id = query_params["audit_id"] if "audit_id" in query_params else None
        clean = query_params["clean"] if "clean" in query_params else None
        export_as_videos = bool(
            strtobool(query_params["export_as_videos"])) if "export_as_videos" in query_params else False
        export_as_channels = bool(
            strtobool(query_params["export_as_channels"])) if "export_as_channels" in query_params else False
        export_as_keywords = bool(
            strtobool(query_params["export_as_keywords"])) if "export_as_keywords" in query_params else False

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
        # pylint: disable=broad-except
        except Exception:
        # pylint: enable=broad-except
            raise ValidationError("Audit with id {} does not exist.".format(audit_id))

        a = AuditExporter.objects.filter(
            audit=audit,
            clean=clean,
            final=True,
            export_as_videos=export_as_videos,
            export_as_channels=export_as_channels,
            export_as_keywords=export_as_keywords,
        )
        if a.count() == 0:
            try:
                a = AuditExporter.objects.get(
                    audit=audit,
                    clean=clean,
                    completed__isnull=True,
                    export_as_videos=export_as_videos,
                    export_as_channels=export_as_channels,
                    export_as_keywords=export_as_keywords,
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
                    export_as_videos=export_as_videos,
                    export_as_channels=export_as_channels,
                    export_as_keywords=export_as_keywords,
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
        if categories.count() > 0:
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
        # pylint: disable=broad-except
        except Exception:
        # pylint: enable=broad-except
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

    def delete_blocklist_channels(self, audit):
        if audit.params.get("override_blocklist"):
            return
        blocklist_channels = BlacklistItem.objects.filter(item_type=1, blocklist=True).values_list('item_id', flat=True)
        bad_channels = AuditChannelProcessor.objects.filter(audit=audit,
                                                            channel__channel_id__in=blocklist_channels)
        bad_channels_videos = AuditVideoProcessor.objects.filter(audit=audit,
                                                            channel__channel_id__in=blocklist_channels)
        blocklist_videos = BlacklistItem.objects.filter(item_type=0, blocklist=True).values_list('item_id', flat=True)
        bad_videos = AuditVideoProcessor.objects.filter(audit=audit,
                                                        video__video_id__in=blocklist_videos)
        bad_channels.delete()
        bad_channels_videos.delete()
        bad_videos.delete()

    def export_videos(self, audit, audit_id=None, clean=None, export=None):
        clean_string = 'none'
        if clean is not None:
            clean_string = 'true' if clean else 'false'
        try:
            name = unicodedata.normalize("NFKD", audit.params['name'].replace("/", "-"))
        # pylint: disable=broad-except
        except Exception:
        # pylint: enable=broad-except
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
        export.set_current_step("getting_categories")
        self.get_categories()
        do_inclusion = False
        if audit.params.get('inclusion') and len(audit.params.get('inclusion')) > 0:
            do_inclusion = True
        do_exclusion = True
        #if audit.params.get('exclusion') and len(audit.params.get('exclusion')) > 0:
        #    do_exclusion = True
        export.set_current_step("delete_blocklist_channels")
        self.delete_blocklist_channels(audit)
        cols = [
            "Video URL",
            "Name",
            "Language",
            "Category",
            "Views",
            "Likes",
            "Dislikes",
            # "Emoji",
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
            "Unique Bad Title Hit Words",
            "Video Count",
            "Brand Safety Score",
            "Made For Kids",
            "Age Restricted",
            "Sentiment",
            "Live Broadcast",
            "Aspect Ratio",
        ]
        export.set_current_step("building_bad_word_category_map")
        try:
            bad_word_categories = set(audit.params['exclusion_category'])
            bad_words_category_mapping = dict()
            if "" in bad_word_categories:
                bad_word_categories.remove("")
            if len(bad_word_categories) > 0:
                cols.extend(bad_word_categories)
                for i in range(len(audit.params['exclusion'])):
                    bad_word = audit.params['exclusion'][i][0]
                    category = audit.params['exclusion_category'][i]
                    bad_words_category_mapping[bad_word] = category
        # pylint: disable=broad-except
        except Exception:
        # pylint: enable=broad-except
            pass
        if audit.params.get('get_tags'):
            cols.extend('tags')
        videos = AuditVideoProcessor.objects.filter(audit_id=audit_id)
        if clean is not None:
            videos = videos.filter(clean=clean)
        auditor = VideoAuditor()
        count = videos.count()
        try:
            max_rows = settings.AUDIT_EXPORT_MAX_VIDEO_ROWS
        except Exception  as e:
            max_rows = self.MAX_ROWS
        if audit.params.get('MAX_VIDEO_ROWS') and audit.params.get('MAX_VIDEO_ROWS') > 0:
            max_rows = audit.params.get('MAX_VIDEO_ROWS')
        if count > max_rows:
            count = max_rows
        num_done = 0
        self.local_file = "export_files/{}".format(uuid4().hex)
        with open(self.local_file, 'w+', newline='') as my_file:
            wr = csv.writer(my_file, quoting=csv.QUOTE_ALL)
            wr.writerow(cols)
            print("EXPORT {}: starting video processing {}".format(export.id, self.local_file))
            export.set_current_step("creating_big_dict")
            stop = False
            for avp in videos:
                if stop:
                    continue
                vid = avp.video
                try:
                    v = vid.auditvideometa
                # pylint: disable=broad-except
                except Exception:
                # pylint: enable=broad-except
                    v = None
                v_channel = vid.channel
                try:
                    acm = v_channel.auditchannelmeta
                except Exception:
                    acm = None
                try:
                    language = self.get_lang(v.language_id)
                # pylint: disable=broad-except
                except Exception:
                # pylint: enable=broad-except
                    language = ""
                try:
                    category = self.get_category(v.category_id)
                # pylint: disable=broad-except
                except Exception:
                # pylint: enable=broad-except
                    category = ""
                try:
                    country = self.get_country(acm.country_id)
                # pylint: disable=broad-except
                except Exception:
                # pylint: enable=broad-except
                    country = ""
                try:
                    channel_lang = self.get_lang(acm.language_id)
                # pylint: disable=broad-except
                except Exception:
                # pylint: enable=broad-except
                    channel_lang = ""
                try:
                    video_count = acm.video_count
                # pylint: disable=broad-except
                except Exception:
                # pylint: enable=broad-except
                    video_count = ""
                try:
                    last_uploaded = acm.last_uploaded.strftime("%m/%d/%Y")
                # pylint: disable=broad-except
                except Exception:
                # pylint: enable=broad-except
                    last_uploaded = ""
                try:
                    last_uploaded_view_count = acm.last_uploaded_view_count
                # pylint: disable=broad-except
                except Exception:
                # pylint: enable=broad-except
                    last_uploaded_view_count = ''
                try:
                    last_uploaded_category = self.get_category(acm.last_uploaded_category_id)
                # pylint: disable=broad-except
                except Exception:
                # pylint: enable=broad-except
                    last_uploaded_category = ''
                try:
                    default_audio_language = self.get_lang(v.default_audio_language_id)
                # pylint: disable=broad-except
                except Exception:
                # pylint: enable=broad-except
                    default_audio_language = ""
                v_word_hits = avp.word_hits
                if do_inclusion:
                    all_good_hit_words, unique_good_hit_words = self.get_hit_words(v_word_hits, clean=True)
                else:
                    all_good_hit_words = ""
                    unique_good_hit_words = ""
                if do_exclusion or (
                    v_word_hits and v_word_hits.get('exclusion') and v_word_hits.get('exclusion') == ['ytAgeRestricted']):
                    all_bad_hit_words, unique_bad_hit_words = self.get_hit_words(v_word_hits, clean=False)
                    title_bad_hit_words = v_word_hits.get('exclusion_title')
                    if title_bad_hit_words:
                        title_bad_hit_words = ",".join(title_bad_hit_words)
                    else:
                        title_bad_hit_words = ""
                else:
                    all_bad_hit_words = ""
                    unique_bad_hit_words = ""
                    title_bad_hit_words = ""
                try:
                    video_audit = auditor.audit_serialized({
                        "id": vid.video_id,
                        "title": v.name,
                        "description": v.description,
                        "tags": v.keywords if v.keywords else [],
                    })
                    video_audit_score = getattr(video_audit, "brand_safety_score").overall_score
                    mapped_score = map_brand_safety_score(video_audit_score)
                # pylint: disable=broad-except
                except Exception:
                # pylint: enable=broad-except
                    mapped_score = ""
                    print("Problem calculating video score")
                try:
                    sentiment = round(v.likes / (v.likes + v.dislikes) * 1.0, 2)
                # pylint: disable=broad-except
                except Exception:
                # pylint: enable=broad-except
                    sentiment = ""
                data = [
                    "https://www.youtube.com/video/" + vid.video_id,
                    v.name if v else "",
                    language,
                    category,
                    v.views if v else "",
                    v.likes if v else "",
                    v.dislikes if v else "",
                    # 'T' if v and v.emoji else 'F',
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
                    title_bad_hit_words,
                    video_count if video_count else "",
                    mapped_score,
                    v.made_for_kids if v else "",
                    "Y" if v and v.age_restricted else "",
                    sentiment,
                    "Y" if v and v.live_broadcast else "",
                    v.aspect_ratio if v and v.aspect_ratio else "",
                ]
                try:
                    if len(bad_word_categories) > 0:
                        bad_word_category_dict = defaultdict(list)
                        bad_words = unique_bad_hit_words.split(",")
                        for word in bad_words:
                            try:
                                word_category = bad_words_category_mapping.get(word)
                                bad_word_category_dict[word_category].append(word)
                            # pylint: disable=broad-except
                            except Exception:
                            # pylint: enable=broad-except
                                pass
                        for category in bad_word_categories:
                            if category in bad_word_category_dict:
                                data.append(len(bad_word_category_dict[category]))
                            else:
                                data.append(0)
                # pylint: disable=broad-except
                except Exception:
                # pylint: enable=broad-except
                    pass
                if audit.params.get('get_tags'):
                    data.extend(v.keywords if v.keywords else "")
                wr.writerow(data)
                num_done += 1
                if export and num_done % 250 == 0:
                    old_percent = export.percent_done
                    export.percent_done = int(1.0 * num_done / count * 100) - 5
                    if export.percent_done < 1:
                        export.percent_done = 1
                    if export.percent_done > old_percent:
                        export.save(update_fields=['percent_done'])
                    print("video export {} at {}.  {}/{}".format(export.id, export.percent_done, num_done, count))
                if num_done >= max_rows or num_done >= count:
                    stop = True
                    continue
        export.set_current_step("preparing_to_move_file")
        with open(self.local_file) as my_file:
            s3_file_name = uuid4().hex
            export.set_current_step("moving_file_to_s3")
            AuditS3Exporter.export_to_s3(my_file.buffer.raw, s3_file_name, file_name)
            export.set_current_step("file_copied")
            if audit and audit.completed:
                audit.params['export_{}'.format(clean_string)] = s3_file_name
                audit.save(update_fields=['params'])
            os.remove(self.local_file)
        return s3_file_name, file_name

    def check_legacy(self, audit):
        empty_channel_avps = AuditVideoProcessor.objects.filter(audit=audit, channel__isnull=True)
        if empty_channel_avps.exists():
            print("doing legacy update on {} channels".format(empty_channel_avps.count()))
            for avp in empty_channel_avps:
                try:
                    avp.channel = avp.video.channel
                    avp.save(update_fields=['channel'])
                # pylint: disable=broad-except
                except Exception:
                # pylint: enable=broad-except
                    pass

    def get_scores_for_channels(self, channel_ids, chunk_size=5000):
        """
        Given a list of Channel ids, return a Channel id -> brand safety score map. Works in chunks of chunk_size
        """
        channel_scores = {}
        channel_manager = ChannelManager(sections=(Sections.BRAND_SAFETY,))
        chunk_count = 0
        for chunk in chunks_generator(channel_ids, size=chunk_size):
            print("doing chunk {}".format(chunk_count))
            chunk_count+=1
            results = channel_manager.get(chunk, skip_none=True)
            for channel in results:
                try:
                    channel_scores[channel.main.id] = channel.brand_safety.overall_score
                except Exception as e:
                    channel_scores[channel.main.id] = None
        return channel_scores

    def export_channels(self, audit, audit_id=None, clean=None, export=None):
        if not audit_id:
            audit_id = audit.id
        clean_string = 'none'
        if clean is not None:
            clean_string = 'true' if clean else 'false'
        try:
            name = unicodedata.normalize("NFKD", audit.params['name'].replace("/", "-"))
        # pylint: disable=broad-except
        except Exception:
        # pylint: enable=broad-except
            name = audit_id
        file_name = 'export_{}_{}_{}_{}.csv'.format(audit_id, name, clean_string, str(export.export_as_channels))
        # If audit already exported, simply generate and return temp link
        exports = AuditExporter.objects.filter(
            audit=audit,
            clean=clean,
            final=True,
            export_as_channels=export.export_as_channels
        )
        if exports.count() > 0:
            return exports[0].file_name, None
        if AuditChannelProcessor.objects.filter(audit_id=audit_id, channel__processed_time__isnull=True).exists():
            raise Exception("Some channels still not processed, can't export yet {}".format(audit_id))
        do_inclusion = False
        if audit.params.get('inclusion') and len(audit.params.get('inclusion')) > 0:
            do_inclusion = True
        #do_exclusion = False
        #if audit.params.get('exclusion') and len(audit.params.get('exclusion')) > 0:
        do_exclusion = True
        export.set_current_step("getting_categories")
        self.get_categories()
        export.set_current_step("delete_blocklist_channels")
        self.delete_blocklist_channels(audit)
        cols = [
            "Channel Title",
            "Channel URL",
            "Views",
            "Subscribers",
            "Subscribers Hidden",
            "Num Videos Checked",
            "Num Videos Total",
            "Country",
            "Language",
            "Primary Video Language",
            "Last Video Upload",
            "Last Video Views",
            "Last Video Category",
            "Num Bad Videos",
            "Num Kids Videos",
            "Num Age Restricted Videos",
            "Unique Exclusion Words (channel)",
            "Unique Exclusion Words (videos)",
            "Exclusion Words (channel)",
            "Exclusion Words (video)",
            "Exclusion Words (video titles)",
            "Inclusion Words (channel)",
            "Inclusion Words (video)",
            "Brand Safety Score",
            "Monetised",
            "Sentiment",
            "Error",
        ]
        try:
            export.set_current_step("building_bad_word_category_map")
            bad_word_categories = set(audit.params['exclusion_category'])
            bad_words_category_mapping = dict()
            if "" in bad_word_categories:
                bad_word_categories.remove("")
            if len(bad_word_categories) > 0:
                cols.extend(sorted(bad_word_categories))
                for i in range(len(audit.params['exclusion'])):
                    bad_word = audit.params['exclusion'][i][0]
                    category = audit.params['exclusion_category'][i]
                    bad_words_category_mapping[bad_word] = category
        # pylint: disable=broad-except
        except Exception:
        # pylint: enable=broad-except
            pass
        good_hit_words = {}
        bad_hit_words = {}
        bad_video_hit_words = {}
        bad_video_title_hit_words = {}
        good_video_hit_words = {}
        bad_videos_count = {}
        kid_videos_count = {}
        age_restricted_videos_count = {}
        video_count = {}
        auditchannelmeta_dict = {}
        channel_ids = []
        export.set_current_step("check_legacy")
        self.check_legacy(audit)
        channels = AuditChannelProcessor.objects.filter(audit_id=audit_id)
        if clean is not None:
            channels = channels.filter(clean=clean)
        export.set_current_step("processing_initial_objs")
        for cid in channels:
            full_channel_id = cid.channel.channel_id
            channel_ids.append(full_channel_id)
            if not auditchannelmeta_dict.get(full_channel_id):
                auditchannelmeta_dict[full_channel_id] = cid.channel.auditchannelmeta
            channel_videos_count = 0
            if auditchannelmeta_dict[full_channel_id].video_count is not None:
                channel_videos_count = auditchannelmeta_dict[full_channel_id].video_count
            if audit.params.get('do_videos'):
                try:
                    if len(cid.word_hits.get('processed_video_ids')) < audit.get_num_videos() <= channel_videos_count:
                        print("re-calculating {}".format(str(cid)))
                        self.aggregate_channel_word_hits(audit=audit, acp=cid)
                    video_count[full_channel_id] = len(cid.word_hits.get('processed_video_ids'))
                # pylint: disable=broad-except
                except Exception:
                # pylint: enable=broad-except
                    pass
            if do_inclusion:
                try:
                    i = cid.word_hits.get('inclusion')
                    if i:
                        good_hit_words[full_channel_id] = set(i)
                    i_v = cid.word_hits.get('inclusion_videos')
                    if i_v:
                        good_video_hit_words[full_channel_id] = set(i_v)
                # pylint: disable=broad-except
                except Exception:
                # pylint: enable=broad-except
                    pass
            try:
                kid_videos_count[full_channel_id] = len(cid.word_hits.get('made_for_kids'))
            # pylint: disable=broad-except
            except Exception:
            # pylint: enable=broad-except
                pass
            try:
                age_restricted_videos_count[full_channel_id] = len(cid.word_hits.get('age_restricted_videos'))
            # pylint: disable=broad-except
            except Exception:
            # pylint: enable=broad-except
                pass
            if do_exclusion:
                try:
                    bad_videos_count[full_channel_id] = len(cid.word_hits.get('bad_video_ids'))
                # pylint: disable=broad-except
                except Exception:
                # pylint: enable=broad-except
                    pass
                try:
                    e = cid.word_hits.get('exclusion')
                    if e:
                        bad_hit_words[full_channel_id] = set(e)
                    e_v = cid.word_hits.get('exclusion_videos')
                    if e_v:
                        bad_video_hit_words[full_channel_id] = set(e_v)
                    e_v = cid.word_hits.get('exclusion_videos_title')
                    if e_v:
                        bad_video_title_hit_words[full_channel_id] = set(e_v)
                # pylint: disable=broad-except
                except Exception:
                # pylint: enable=broad-except
                    pass
        export.set_current_step("getting_channel_scores")
        print("EXPORT: getting channel scores: starting")
        try:
            channel_scores = self.get_scores_for_channels(channel_ids)
            print("EXPORT: getting channel scores: done")
        except Exception:
            channel_scores = {}
            print("EXPORT: problem getting scores, connection issue")
        count = channels.count()
        num_done = 0
        self.local_file = "export_files/{}".format(uuid4().hex)
        with open(self.local_file, 'w+', newline='') as my_file:
            wr = csv.writer(my_file, quoting=csv.QUOTE_ALL)
            wr.writerow(cols)
            print("EXPORT: starting channel processing of export {}".format(export.id))
            export.set_current_step("creating_big_dict")
            for db_channel in channels:
                channel = db_channel.channel
                v = auditchannelmeta_dict.get(channel.channel_id)
                if not v:
                    v = channel.auditchannelmeta
                try:
                    language = self.get_lang(v.language_id)
                # pylint: disable=broad-except
                except Exception:
                # pylint: enable=broad-except
                    language = ""
                try:
                    country = self.get_country(v.country_id)
                # pylint: disable=broad-except
                except Exception:
                # pylint: enable=broad-except
                    country = ""
                try:
                    last_category = self.get_category(v.last_uploaded_category_id)
                # pylint: disable=broad-except
                except Exception:
                # pylint: enable=broad-except
                    last_category = ""
                mapped_score = channel_scores.get(channel.channel_id, None)
                try:
                    error_str = db_channel.word_hits.get('error')
                    if not error_str:
                        error_str = ""
                # pylint: disable=broad-except
                except Exception:
                # pylint: enable=broad-except
                    error_str = ""
                try:
                    primary_video_language = self.get_lang(v.primary_video_language_id)
                # pylint: disable=broad-except
                except Exception:
                # pylint: enable=broad-except
                    primary_video_language = ""
                try:
                    sentiment = round(v.likes / (v.likes + v.dislikes) * 1.0, 2)
                # pylint: disable=broad-except
                except Exception:
                # pylint: enable=broad-except
                    sentiment = ""
                data = [
                    v.name,
                    "https://www.youtube.com/channel/" + channel.channel_id,
                    v.view_count if v.view_count else "",
                    v.subscribers,
                    'Y' if v.hidden_subscriber_count else '',
                    video_count.get(channel.channel_id) if video_count.get(channel.channel_id) else 0,
                    v.video_count if v.video_count is not None else "",
                    country,
                    language,
                    primary_video_language,
                    v.last_uploaded.strftime("%Y/%m/%d") if v.last_uploaded else "",
                    v.last_uploaded_view_count if v.last_uploaded_view_count else "",
                    last_category,
                    bad_videos_count.get(channel.channel_id) if bad_videos_count.get(channel.channel_id) else 0,
                    kid_videos_count.get(channel.channel_id) if kid_videos_count.get(channel.channel_id) else 0,
                    age_restricted_videos_count.get(channel.channel_id) if age_restricted_videos_count.get(
                        channel.channel_id) else 0,
                    len(bad_hit_words.get(channel.channel_id)) if bad_hit_words.get(channel.channel_id) else 0,
                    len(bad_video_hit_words.get(channel.channel_id)) if bad_video_hit_words.get(
                        channel.channel_id) else 0,
                    ','.join(bad_hit_words.get(channel.channel_id)) if bad_hit_words.get(channel.channel_id) else "",
                    ','.join(bad_video_hit_words.get(channel.channel_id)) if bad_video_hit_words.get(
                        channel.channel_id) else "",
                    ','.join(bad_video_title_hit_words.get(channel.channel_id)) if bad_video_title_hit_words.get(
                        channel.channel_id) else "",
                    ','.join(good_hit_words.get(channel.channel_id)) if good_hit_words.get(channel.channel_id) else "",
                    ','.join(good_video_hit_words.get(channel.channel_id)) if good_video_hit_words.get(
                        channel.channel_id) else "",
                    mapped_score if mapped_score else "",
                    'true' if v.monetised else "",
                    sentiment,
                    error_str,
                ]
                try:
                    if len(bad_word_categories) > 0:
                        bad_word_category_dict = defaultdict(list)
                        bad_words = set()
                        if channel.channel_id in bad_hit_words:
                            bad_words = bad_words.union(bad_hit_words[channel.channel_id])
                        if channel.channel_id in bad_video_hit_words:
                            bad_words = bad_words.union(bad_video_hit_words[channel.channel_id])
                        for word in bad_words:
                            try:
                                word_category = bad_words_category_mapping.get(word)
                                bad_word_category_dict[word_category].append(word)
                            # pylint: disable=broad-except
                            except Exception:
                            # pylint: enable=broad-except
                                pass
                        for category in sorted(bad_word_categories):
                            if category in bad_word_category_dict:
                                data.append(len(bad_word_category_dict[category]))
                            else:
                                data.append(0)
                # pylint: disable=broad-except
                except Exception:
                # pylint: enable=broad-except
                    pass
                wr.writerow(data)
                num_done += 1
                if export and num_done % 250 == 0:
                    old_percent = export.percent_done
                    export.percent_done = int(num_done / count * 100.0) - 5
                    if export.percent_done < 0:
                        export.percent_done = 0
                    if export.percent_done > old_percent:
                        export.save(update_fields=['percent_done'])
                    print("channel export {} at {}, {}/{}".format(export.id, export.percent_done, num_done, count))
        export.set_current_step("preparing_to_move_file")
        with open(self.local_file) as myfile:
            s3_file_name = uuid4().hex
            try:
                export.set_current_step("moving_file_to_s3")
                AuditS3Exporter.export_to_s3(myfile.buffer.raw, s3_file_name, file_name)
                os.remove(self.local_file)
                export.set_current_step("file_copied")
                print("copied {} to S3".format(file_name))
            except Exception as e:
                os.remove(self.local_file)
                raise Exception("problem copying file {} to S3: {}".format(file_name, str(e)))
            if audit and audit.completed:
                audit.params['export_{}'.format(clean_string)] = s3_file_name
                audit.save(update_fields=['params'])
        return s3_file_name, file_name

    def aggregate_channel_word_hits(self, audit, acp):
        if not isinstance(audit, AuditProcessor) or \
                not isinstance(acp, AuditChannelProcessor) or \
                not audit.params.get("do_videos"):
            return
        avps = AuditVideoProcessor.objects.filter(audit=audit, channel=acp.channel)
        if len(avps) > len(acp.word_hits.get('processed_video_ids')):
            for avp in avps:
                if BlacklistItem.get(avp.video.video_id, BlacklistItem.VIDEO_ITEM):
                    self.append_to_channel(acp, avp, [avp.video_id], "bad_video_ids")
                else:
                    db_video_meta = AuditVideoMeta.objects.get(video=avp.video)
                    self.append_to_channel(acp, [avp.video_id], "processed_video_ids")
                    if db_video_meta.made_for_kids:
                        self.append_to_channel(acp, [avp.video_id], "made_for_kids")
                    if db_video_meta.age_restricted:
                        self.append_to_channel(acp, [avp.video_id], "age_restricted_videos")
                        self.append_to_channel(acp, [avp.video_id], "bad_video_ids")
                    if "inclusion" in avp.word_hits and len(avp.word_hits["inclusion"]) > 0:
                        self.append_to_channel(acp, avp.word_hits["inclusion"], "inclusion_videos")
                    if "exclusion" in avp.word_hits and len(avp.word_hits["exclusion"]) > 0:
                        self.append_to_channel(acp, avp.word_hits["exclusion"], "exclusion_videos")
                        self.append_to_channel(acp, [avp.video_id], "bad_video_ids")
                    if "exclusion_title" in avp.word_hits and len(avp.word_hits["exclusion_title"]) > 0:
                        self.append_to_channel(acp, avp.word_hits["exclusion_title"], "exclusion_videos")
                        self.append_to_channel(acp, [avp.video_id], "bad_video_ids")

    def append_to_channel(self, acp, hits, node):
        """
        This is a helper function to the aggregate_channel_word_hits function.
        It adds the items in "hits" to the list "acp.word_hits[node]", eliminates duplicates,
        and saves the acp object after the change
        """
        if node not in acp.word_hits:
            acp.word_hits[node] = []
        node_items_set = set(acp.word_hits[node])
        node_items_set.update(hits)
        acp.word_hits[node] = list(node_items_set)
        acp.save(update_fields=["word_hits"])

    def export_keywords(self, audit, audit_id=None, export=None):
        if not audit_id:
            audit_id = audit.id
        file_name = 'keywords_{}.csv'.format(audit_id)
        # If audit already exported, simply generate and return temp link
        exports = AuditExporter.objects.filter(
            audit=audit,
            final=True,
            export_as_keywords=True
        )
        if exports.count() > 0:
            return exports[0].file_name, None, 0
        cols = [
            "Hit Word",
            "Count",
            "Word Type",
        ]
        rows = [cols]
        videos = AuditVideoProcessor.objects.filter(audit_id=audit_id)
        bad_words = {}
        good_words = {}
        count = 0
        total = videos.count()
        for video in videos:
            hits = video.word_hits
            if hits:
                e = hits.get('exclusion', [])
                if e:
                    for word in e:
                        if word not in bad_words:
                            bad_words[word] = 1
                        else:
                            bad_words[word] += 1
                e = hits.get('inclusion', [])
                if e:
                    for word in e:
                        if word not in good_words:
                            good_words[word] = 1
                        else:
                            good_words[word] += 1
            count += 1
            if count % 250 == 0:
                export.percent_done = round(count / total * 100 * 0.4)
                export.save(update_fields=['percent_done'])

        for word, count in bad_words.items():
            rows.append([word, count, 'e'])
        export.percent_done = 60
        export.save(update_fields=['percent_done'])
        for word, count in good_words.items():
            rows.append([word, count, 'i'])
        export.percent_done = 80
        export.save(update_fields=['percent_done'])
        word_counter = len(rows)
        with open(file_name, 'w+', newline='') as myfile:
            wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
            wr.writerows(rows)
        export.percent_done = 100
        export.save(update_fields=['percent_done'])
        with open(file_name) as myfile:
            s3_file_name = uuid4().hex
            download_file_name = file_name
            AuditS3Exporter.export_to_s3(myfile.buffer.raw, s3_file_name, download_file_name)
            os.remove(myfile.name)
        return s3_file_name, download_file_name, word_counter

    def get_hit_words(self, hits, clean=None):
        uniques = set()
        words_to_use = 'exclusion'
        if clean is None or clean == True:
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

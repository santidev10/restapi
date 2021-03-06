import logging
import re
from collections import defaultdict
from datetime import datetime
from datetime import timedelta

import requests
from dateutil.parser import parse
from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils import timezone
from emoji import UNICODE_EMOJI
from pid import PidFile
from threading import Thread

from audit_tool.models import AuditCategory
from audit_tool.models import AuditChannel
from audit_tool.models import AuditChannelMeta
from audit_tool.models import AuditChannelProcessor
from audit_tool.models import AuditExporter
from audit_tool.models import AuditLanguage
from audit_tool.models import AuditProcessor
from audit_tool.models import AuditVideo
from audit_tool.models import AuditVideoMeta
from audit_tool.models import AuditVideoProcessor
from audit_tool.models import BlacklistItem
from audit_tool.utils.regex_trie import get_optimized_regex
from utils.lang import fasttext_lang
from utils.lang import remove_mentions_hashes_urls
from utils.utils import remove_tags_punctuation

logger = logging.getLogger(__name__)

"""
requirements:
    we receive a list of video URLs as a 'seed list'.
    we receive a list of blacklist keywords
    we receive a list of inclusion keywords
process:
    we use the seed list of video URL's to retrieve 'recommended videos' from YT.
    for each video on the recommended list we check that it doesnt contain
    blacklist keywords, and that it includes 'inclusion keywords' if present.
    once the # of videos reaches the max_recommended value it stops.
"""


# pylint: disable=too-many-instance-attributes
class Command(BaseCommand):
    MAX_VIDS = 1000000
    keywords = []
    inclusion_list = None
    exclusion_list = None
    categories = {}
    audit = None
    NUM_THREADS = settings.AUDIT_RECO_NUM_THREADS
    DATA_API_KEY = settings.YOUTUBE_API_DEVELOPER_KEY
    DATA_RECOMMENDED_API_URL = "https://www.googleapis.com/youtube/v3/search" \
                               "?key={key}&part=id,snippet&relatedToVideoId={id}" \
                               "&type=video&maxResults=50{language}"
    DATA_VIDEO_API_URL = "https://www.googleapis.com/youtube/v3/videos" \
                         "?key={key}&part=id,status,snippet,statistics,contentDetails,player&id={id}"
    DATA_CHANNEL_API_URL = "https://www.googleapis.com/youtube/v3/channels" \
                           "?key={key}&part=id,statistics,brandingSettings&id={id}"
    CATEGORY_API_URL = "https://www.googleapis.com/youtube/v3/videoCategories" \
                       "?key={key}&part=id,snippet&id={id}"

    def __init__(self, stdout=None, stderr=None, no_color=False, force_color=False):
        super(Command, self).__init__(stdout=stdout, stderr=stderr, no_color=no_color, force_color=force_color)
        self.thread_id = None
        self.machine_number = None
        self.db_languages = None
        self.db_language_ids = None
        self.language = None
        self.location = None
        self.location_radius = None
        self.category = None
        self.related_audits = None
        self.exclusion_hit_count = None
        self.inclusion_hit_count = None
        self.include_unknown_views = None
        self.include_unknown_likes = None
        self.min_date = None
        self.min_views = None
        self.min_likes = None
        self.max_dislikes = None

    def add_arguments(self, parser):
        parser.add_argument("thread_id", type=int)

    # this is the primary method to call to trigger the entire audit sequence
    def handle(self, *args, **options):
        self.thread_id = options.get("thread_id")
        if not self.thread_id:
            self.thread_id = 0
        try:
            self.machine_number = settings.AUDIT_MACHINE_NUMBER
        # pylint: disable=broad-except
        except Exception:
        # pylint: enable=broad-except
            self.machine_number = 0
        with PidFile(piddir="pids", pidname="recommendation_{}.pid".format(self.thread_id)):
            try:
                self.audit = AuditProcessor.objects \
                    .filter(temp_stop=False,
                            completed__isnull=True,
                            audit_type=0,
                            seed_status=2,
                            source=0) \
                    .order_by("pause", "id")[self.machine_number]
                self.load_audit_params()
            # pylint: disable=broad-except
            except Exception as e:
            # pylint: enable=broad-except
                logger.exception(e)
                raise Exception("no audits to process at present")
            self.process_audit()

    def get_lang_by_id(self, l_id):
        if l_id not in self.db_language_ids:
            self.db_language_ids[l_id] = AuditLanguage.objects.get(id=l_id).language.lower()
        return self.db_language_ids[l_id]

    def load_audit_params(self):
        self.db_languages = {}
        self.db_language_ids = {}
        self.language = self.audit.params.get("language")
        self.location = self.audit.params.get("location")
        self.location_radius = self.audit.params.get("location_radius")
        self.category = self.audit.params.get("category")
        self.related_audits = self.audit.params.get("related_audits")
        self.exclusion_hit_count = self.audit.params.get("exclusion_hit_count")
        self.inclusion_hit_count = self.audit.params.get("inclusion_hit_count")
        self.include_unknown_views = self.audit.params.get("include_unknown_views")
        self.include_unknown_likes = self.audit.params.get("include_unknown_likes")
        if not self.exclusion_hit_count:
            self.exclusion_hit_count = 1
        else:
            self.exclusion_hit_count = int(self.exclusion_hit_count)
        if not self.inclusion_hit_count:
            self.inclusion_hit_count = 1
        else:
            self.inclusion_hit_count = int(self.inclusion_hit_count)
        self.min_date = self.audit.params.get("min_date")
        if self.min_date:
            self.min_date = datetime.strptime(self.min_date, "%m/%d/%Y")
        self.min_views = int(self.audit.params.get("min_views")) if self.audit.params.get("min_views") else None
        self.min_likes = int(self.audit.params.get("min_likes")) if self.audit.params.get("min_likes") else None
        self.max_dislikes = int(self.audit.params.get("max_dislikes")) if self.audit.params.get(
            "max_dislikes") else None

    def process_audit(self):
        self.load_inclusion_list()
        self.load_exclusion_list()
        if not self.audit.started:
            self.audit.started = timezone.now()
            self.audit.save(update_fields=["started"])
        pending_videos = self.check_complete()
        num = 50
        start = self.thread_id * num
        threads = []
        for video in pending_videos[start:start + num]:
            t = Thread(target=self.do_recommended_api_call, args=(video,))
            threads.append(t)
            t.start()
            if len(threads) >= self.NUM_THREADS:
                for t in threads:
                    t.join()
                threads = []
            # self.do_recommended_api_call(video)
        if len(threads) > 0:
            for t in threads:
                t.join()
        self.audit.updated = timezone.now()
        self.audit.save(update_fields=["updated"])
        self.check_complete()

    def check_complete(self):
        pending_videos = AuditVideoProcessor.objects.filter(audit=self.audit)
        if pending_videos.count() > self.MAX_VIDS:
            self.complete_audit()
        else:
            max_recommended_type = self.audit.params.get("max_recommended_type")
            if not max_recommended_type:
                max_recommended_type = "video"
            if max_recommended_type == "video" \
                and pending_videos.filter(clean=True).count() > self.audit.max_recommended:
                self.complete_audit()
            elif max_recommended_type == "channel":
                unique_channels = AuditChannelProcessor.objects.filter(audit=self.audit)
                # pending_videos.filter(clean=True).values("channel_id").distinct()
                if unique_channels.count() > self.audit.max_recommended:
                    self.complete_audit()
        pending_videos = pending_videos.filter(processed__isnull=True)
        if pending_videos.count() == 0:  # we've processed ALL of the items so we close the audit
            self.complete_audit()
        else:
            pending_videos = pending_videos.order_by("id")
        return pending_videos

    def complete_audit(self):
        if self.thread_id == 0:
            self.audit.completed = timezone.now()
            self.audit.pause = 0
            self.audit.save(update_fields=["completed", "pause"])
            print("Audit completed, all videos processed")
            max_recommended_type = self.audit.params.get("max_recommended_type")
            export_as_channels = False
            if max_recommended_type and max_recommended_type == "channel":
                export_as_channels = True
            AuditExporter.objects.create(
                audit=self.audit,
                owner_id=None,
                clean=True,
                export_as_channels=export_as_channels,
            )
        raise Exception("Audit completed, all videos processed")

    # pylint: disable=too-many-branches,too-many-statements
    def do_recommended_api_call(self, avp):
        video = avp.video
        if video.video_id is None:
            avp.clean = False
            avp.processed = timezone.now()
            avp.save(update_fields=["clean", "processed"])
            return
        url = self.DATA_RECOMMENDED_API_URL.format(
            key=self.DATA_API_KEY,
            id=video.video_id,
            language="&relevanceLanguage={}".format(self.language[0]) if self.language and len(
                self.language) == 1 else "",
            location="&location={}".format(self.location) if self.location else "",
            location_radius="&locationRadius={}mi".format(self.location_radius) if self.location_radius else ""
        )
        r = requests.get(url)
        data = r.json()
        if "error" in data:
            if (data["error"].get("code") and str(data["error"]["code"]) == "404") or data["error"]["message"] in ["Invalid video.", "Not Found", "Requested entity was not found."]:
                avp.processed = timezone.now()
                avp.clean = False
                avp.save(update_fields=["clean", "processed"])
                return
            if data["error"]["message"] == "Invalid relevance language.":
                self.audit.params["error"] = "Invalid relevance language."
                self.audit.completed = timezone.now()
                self.audit.pause = 0
                self.audit.save()
                raise Exception("problem with relevance language.")
        try:
            d = data["items"]
        # pylint: disable=broad-except
        except Exception:
        # pylint: enable=broad-except
            print(str(data))
            raise Exception("problem with API response {}".format(str(data)))
        for i in d:
            if not i.get("snippet"):
                continue
            db_video = AuditVideo.get_or_create(i["id"]["videoId"])
            db_video_meta, _ = AuditVideoMeta.objects.get_or_create(video=db_video)
            db_video_meta.name = i["snippet"]["title"]
            db_video_meta.description = i["snippet"]["description"]
            try:
                db_video_meta.publish_date = parse(i["snippet"]["publishedAt"])
            # pylint: disable=broad-except
            except Exception:
            # pylint: enable=broad-except
                print("no video publish date")
            if not db_video.processed_time or db_video.processed_time < (timezone.now() - timedelta(days=30)):
                self.do_video_metadata_api_call(db_video_meta, db_video.video_id)
                db_video.processed_time = timezone.now()
                db_video.save(update_fields=["processed_time"])
            channel = AuditChannel.get_or_create(i["snippet"]["channelId"], add_meta=True)
            db_video_meta.save()
            if db_video.channel != channel:
                db_video.channel = channel
                db_video.save(update_fields=["channel"])
            db_channel_meta, _ = AuditChannelMeta.objects.get_or_create(channel=channel)
            if not db_channel_meta.name or db_channel_meta.name != i["snippet"]["channelTitle"]:
                db_channel_meta.name = i["snippet"]["channelTitle"]
                db_channel_meta.save(update_fields=["name"])
            is_clean, hits = self.check_video_is_clean(db_video_meta)
            if is_clean:
                if self.check_video_matches_criteria(db_video_meta, db_video):
                    v, _ = AuditVideoProcessor.objects.get_or_create(
                        video=db_video,
                        audit=self.audit
                    )
                    update_fields = ["word_hits", "clean"]
                    v.word_hits = hits
                    if not v.video_source:
                        v.video_source = video
                        update_fields.append("video_source")
                    if not v.channel and channel:
                        v.channel = channel
                        update_fields.append("channel")
                    v.clean = self.check_video_matches_minimums(db_video_meta)
                    v.save(update_fields=update_fields)
                    if v.clean:
                        AuditChannelProcessor.objects.get_or_create(
                            audit=self.audit,
                            channel=channel
                        )
        avp.processed = timezone.now()
        update_fields = ["processed"]
        if not avp.channel:
            avp.channel = video.channel
            update_fields.append("channel")
        avp.save(update_fields=update_fields)
    # pylint: enable=too-many-branches,too-many-statements

    def check_video_matches_criteria(self, db_video_meta, db_video):
        if self.language:
            if not db_video_meta.language or self.get_lang_by_id(db_video_meta.language_id) not in self.language:
                return False
        if self.category:
            try:
                if int(db_video_meta.category.category) not in self.category:
                    return False
            except Exception:
                pass
        if self.related_audits:
            if AuditVideoProcessor.objects.filter(video_id=db_video.id, audit_id__in=self.related_audits,
                                                  clean=True).exists():
                return False
        if not self.audit.params.get("override_blocklist"):
            if BlacklistItem.get(db_video.channel.channel_id,
                                 BlacklistItem.CHANNEL_ITEM):  # if videos channel is blacklisted
                return False
            if BlacklistItem.get(db_video.video_id, BlacklistItem.VIDEO_ITEM):  # if video is blacklisted
                return False
        return True

    def check_video_matches_minimums(self, db_video_meta):
        if self.min_views:
            if db_video_meta.views < self.min_views:
                if db_video_meta.views == 0 and self.include_unknown_views:
                    pass
                else:
                    return False
        if self.min_date:
            if db_video_meta.publish_date.replace(tzinfo=None) < self.min_date:
                return False
        if self.min_likes:
            if db_video_meta.likes < self.min_likes:
                if db_video_meta.likes == 0 and self.include_unknown_likes:
                    pass
                else:
                    return False
        if self.max_dislikes:
            if db_video_meta.dislikes > self.max_dislikes:
                return False
        return True

    def check_video_is_clean(self, db_video_meta):
        hits = {}
        full_string = remove_tags_punctuation("{} {} {}".format(
            "" if not db_video_meta.name else db_video_meta.name,
            "" if not db_video_meta.description else db_video_meta.description,
            "" if not db_video_meta.keywords else db_video_meta.keywords,
        ))
        if db_video_meta.age_restricted:
            return False, ["ytAgeRestricted"]
        if self.inclusion_list:
            is_there, b_hits = self.check_exists(full_string.lower(), self.inclusion_list,
                                                 count=self.inclusion_hit_count)
            hits["inclusion"] = b_hits
            if not is_there:
                return False, hits
        if self.exclusion_list:
            try:
                language = self.get_lang_by_id(db_video_meta.language_id)
            # pylint: disable=broad-except
            except Exception:
            # pylint: enable=broad-except
                language = ""
            if language not in self.exclusion_list and "" not in self.exclusion_list:
                return True, hits
            is_there = False
            b_hits = []
            if self.exclusion_list.get(language):
                is_there, b_hits = self.check_exists(full_string.lower(), self.exclusion_list[language],
                                                     count=self.exclusion_hit_count)
            if language != "" and self.exclusion_list.get(""):
                is_there_b, b_hits_b = self.check_exists(full_string.lower(), self.exclusion_list[""],
                                                         count=self.exclusion_hit_count)
                if not is_there and is_there_b:
                    is_there = True
                    b_hits = b_hits_b
                elif b_hits and b_hits_b:
                    b_hits = b_hits + b_hits_b
            if is_there:
                hits["exclusion"] = b_hits
                return False, hits
        return True, hits

    def audit_video_meta_for_emoji(self, db_video_meta):
        if db_video_meta.name and self.contains_emoji(db_video_meta.name):
            return True
        if db_video_meta.description and self.contains_emoji(db_video_meta.description):
            return True
        if db_video_meta.keywords and self.contains_emoji(db_video_meta.keywords):
            return True
        return False

    def audit_channel_meta_for_emoji(self, db_channel_meta):
        if db_channel_meta.name and self.contains_emoji(db_channel_meta.name):
            return True
        if db_channel_meta.description and self.contains_emoji(db_channel_meta.description):
            return True
        if db_channel_meta.keywords and self.contains_emoji(db_channel_meta.keywords):
            return True
        return False

    def contains_emoji(self, string):
        for character in string:
            if character in UNICODE_EMOJI:
                return True
        return False

    # pylint: disable=too-many-branches,too-many-statements
    def do_video_metadata_api_call(self, db_video_meta, video_id):
        try:
            url = self.DATA_VIDEO_API_URL.format(key=self.DATA_API_KEY, id=video_id)
            r = requests.get(url)
            data = r.json()
            if r.status_code != 200:
                logger.info("problem with api call for video %s", video_id)
                return
            try:
                i = data["items"][0]
            # pylint: disable=broad-except
            except Exception:
            # pylint: enable=broad-except
                print("problem getting video {}".format(video_id))
                return
            db_video_meta.description = i["snippet"].get("description")
            keywords = i["snippet"].get("tags")
            if keywords:
                db_video_meta.keywords = " ".join(keywords)
            category_id = i["snippet"].get("categoryId")
            if category_id:
                if not category_id in self.categories:
                    self.categories[category_id], _ = AuditCategory.objects.get_or_create(category=category_id)
            db_video_meta.category = self.categories[category_id]
            try:
                html = i["player"]["embedHtml"]
                width = int(html.split("width=\"")[1].split("\"")[0])
                height = int(html.split("height=\"")[1].split("\"")[0])
                aspect_ratio = round(width / height * 1.0, 2)
                db_video_meta.aspect_ratio = aspect_ratio
            except Exception:
                pass
            try:
                if i["snippet"]["liveBroadcastContent"] in ["live", "upcoming"]:
                    db_video_meta.live_broadcast = True
                else:
                    db_video_meta.live_broadcast = False
            except Exception:
                pass
            try:
                db_video_meta.views = int(i["statistics"]["viewCount"])
            # pylint: disable=broad-except
            except Exception:
            # pylint: enable=broad-except
                pass
            try:
                db_video_meta.likes = int(i["statistics"]["likeCount"])
            # pylint: disable=broad-except
            except Exception:
            # pylint: enable=broad-except
                pass
            try:
                db_video_meta.dislikes = int(i["statistics"]["dislikeCount"])
            # pylint: disable=broad-except
            except Exception:
            # pylint: enable=broad-except
                pass
            try:
                db_video_meta.made_for_kids = i["status"]["madeForKids"]
            # pylint: disable=broad-except
            except Exception:
            # pylint: enable=broad-except
                pass
            db_video_meta.emoji = self.audit_video_meta_for_emoji(db_video_meta)
            if "defaultAudioLanguage" in i["snippet"]:
                try:
                    lang = i["snippet"]["defaultAudioLanguage"]
                    if lang not in self.db_languages:
                        self.db_languages[lang] = AuditLanguage.from_string(lang)
                    db_video_meta.default_audio_language = self.db_languages[lang]
                # pylint: disable=broad-except
                except Exception:
                # pylint: enable=broad-except
                    pass
            try:
                db_video_meta.duration = i["contentDetails"]["duration"]
            # pylint: disable=broad-except
            except Exception:
            # pylint: enable=broad-except
                pass
            try:
                if i["contentDetails"]["contentRating"]["ytRating"] == "ytAgeRestricted":
                    db_video_meta.age_restricted = True
            # pylint: disable=broad-except
            except Exception:
            # pylint: enable=broad-except
                pass
            str_long = db_video_meta.name
            if db_video_meta.keywords:
                str_long = "{} {}".format(str_long, db_video_meta.keywords)
            if db_video_meta.description:
                str_long = "{} {}".format(str_long, db_video_meta.description)
            db_video_meta.language = self.calc_language(str_long)
        # pylint: disable=broad-except
        except Exception as e:
        # pylint: enable=broad-except
            logger.exception(e)
        return
    # pylint: enable=too-many-branches,too-many-statements

    def calc_language(self, data):
        try:
            data = remove_mentions_hashes_urls(data).lower()
            l = fasttext_lang(data)
            if l:
                if l not in self.db_languages:
                    self.db_languages[l] = AuditLanguage.from_string(l)
                return self.db_languages[l]
        # pylint: disable=broad-except
        except Exception:
        # pylint: enable=broad-except
            pass
        return None

    def load_inclusion_list(self):
        if self.inclusion_list:
            return
        input_list = self.audit.params.get("inclusion")
        if not input_list:
            return
        self.inclusion_list = get_optimized_regex(words_list=input_list, remove_tags_punctuation_from_words=True)

    def load_exclusion_list(self):
        if self.exclusion_list:
            return
        input_list = self.audit.params.get("exclusion")
        if not input_list:
            return
        language_keywords_dict = defaultdict(list)
        exclusion_list = {}
        for row in input_list:
            word = remove_tags_punctuation(row[0])
            try:
                language = row[2].lower()
                if language == "un":
                    language = ""
            # pylint: disable=broad-except
            except Exception:
            # pylint: enable=broad-except
                language = ""
            language_keywords_dict[language].append(word)
        for lang, keywords in language_keywords_dict.items():
            exclusion_list[lang] = get_optimized_regex(words_list=keywords)
        self.exclusion_list = exclusion_list

    def check_exists(self, text, exp, count=1):
        keywords = re.findall(exp, remove_tags_punctuation(text))
        if len(keywords) >= count:
            return True, keywords
        return False, None

    def get_categories(self):
        categories = AuditCategory.objects.filter(category_display__isnull=True).values_list("category", flat=True)
        url = self.CATEGORY_API_URL.format(key=self.DATA_API_KEY, id=",".join(categories))
        r = requests.get(url)
        data = r.json()
        for i in data["items"]:
            AuditCategory.objects.filter(category=i["id"]).update(category_display=i["snippet"]["title"])
# pylint: enable=too-many-instance-attributes

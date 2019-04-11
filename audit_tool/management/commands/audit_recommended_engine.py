import logging
from django.conf import settings
import re
import requests
from django.utils import timezone
import langid
from dateutil.parser import parse
from emoji import UNICODE_EMOJI
from audit_tool.models import AuditCategory
from audit_tool.models import AuditChannel
from audit_tool.models import AuditChannelMeta
from audit_tool.models import AuditCountry
from audit_tool.models import AuditLanguage
from audit_tool.models import AuditProcessor
from audit_tool.models import AuditVideo
from audit_tool.models import AuditVideoMeta
from audit_tool.models import AuditVideoProcessor
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

class AuditRecommendationEngine():
    keywords = []
    inclusion_list = None
    exclusion_list = None
    categories = {}
    audit = None
    DATA_API_KEY = settings.YOUTUBE_API_DEVELOPER_KEY
    DATA_RECOMMENDED_API_URL = "https://www.googleapis.com/youtube/v3/search" \
                               "?key={key}&part=id,snippet&relatedToVideoId={id}&type=video"
    DATA_VIDEO_API_URL =    "https://www.googleapis.com/youtube/v3/videos" \
                            "?key={key}&part=id,snippet,statistics&id={id}"
    DATA_CHANNEL_API_URL = "https://www.googleapis.com/youtube/v3/channels" \
                         "?key={key}&part=id,statistics,brandingSettings&id={id}"

    # this is the primary method to call to trigger the entire audit sequence
    def get_current_audit_to_process(self):
        try:
            self.audit = AuditProcessor.objects.filter(completed__isnull=True).order_by("id")[0]
        except Exception as e:
            logger.exception(e)
        self.process_audit()

    def process_audit(self):
        self.load_inclusion_list()
        self.load_exclusion_list()
        pending_videos = AuditVideoProcessor.objects.filter(audit=self.audit)
        if pending_videos.count() == 0:
            pending_videos = self.process_seed_list()
        else:
            pending_videos = pending_videos.filter(processed__isnull=True).order_by("id")
            if pending_videos.count() == 0:  # we've processed ALL of the items so we close the audit
                self.audit.completed = timezone.now()
                self.audit.save()
                logger.info("Audit completed, all videos processed")
                raise Exception("Audit completed, all videos processed")
        for video in pending_videos:
            self.do_recommended_api_call(video)
        self.audit.updated = timezone.now()
        self.audit.save()
        if AuditVideoProcessor.objects.filter(audit=self.audit).count() >= self.audit.max_recommended:
            self.audit.completed = timezone.now()
            self.audit.save()
            logger.info("Audit completed {}".format(self.audit.id))
            raise Exception("Audit completed {}".format(self.audit.id))
        else:
            logger.info("Done one step, continuing audit {}.".format(self.audit.id))
            self.process_audit()

    def process_seed_list(self):
        seed_list = self.audit.params.get('videos')
        if not seed_list:
            logger.info("seed list is empty for this audit. {}".format(self.audit.id))
            raise Exception("seed list is empty for this audit. {}".format(self.audit.id))
        vids = []
        for seed in seed_list:
            v_id = seed.split("/")[-1]
            if '?v=' in  v_id:
                v_id = v_id.split("v=")[-1]
            video = AuditVideo.get_or_create(v_id)
            avp, _ = AuditVideoProcessor.objects.get_or_create(
                audit=self.audit,
                video=video,
            )
            vids.append(avp)
        return vids

    def do_recommended_api_call(self, avp):
        video = avp.video
        url = self.DATA_RECOMMENDED_API_URL.format(key=self.DATA_API_KEY, id=video.video_id)
        r = requests.get(url)
        data = r.json()
        for i in data['items']:
            db_video = AuditVideo.get_or_create(i['id']['videoId'])
            db_video_meta, _ = AuditVideoMeta.objects.get_or_create(video=db_video)
            db_video_meta.name = i['snippet']['title']
            db_video_meta.description = i['snippet']['description']
            db_video.publish_date = parse(i['snippet']['publishedAt'])
            if not db_video_meta.keywords:
                self.do_video_metadata_api_call(db_video_meta, db_video.video_id)
            db_video.channel = AuditChannel.get_or_create(i['snippet']['channelId'])
            db_video_meta.save()
            db_video.save()
            db_channel_meta, _ = AuditChannelMeta.objects.get_or_create(
                    channel=db_video.channel,
            )
            db_channel_meta.name = i['snippet']['channelTitle']
            if not db_channel_meta.keywords:
                self.do_channel_metadata_api_call(db_channel_meta, i['snippet']['channelId'])
            db_channel_meta.save()
            if self.check_video_is_clean(db_video_meta):
                v, _  = AuditVideoProcessor.objects.get_or_create(
                    video=db_video,
                    audit=self.audit
                )
                if not v.video_source:
                    v.video_source = video
                    v.save()
        avp.processed = timezone.now()
        avp.save(update_fields=['processed'])

    def check_video_is_clean(self, db_video_meta):
        full_string = "{} {} {}".format(
            '' if not db_video_meta.name else db_video_meta.name,
            '' if not db_video_meta.description else db_video_meta.description,
            '' if not db_video_meta.keywords else db_video_meta.keywords,
        )
        if self.inclusion_list and not self.check_exists(full_string, self.inclusion_list):
                return False
        if self.exclusion_list and self.check_exists(full_string, self.exclusion_list):
                return False
        return True

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

    def contains_emoji(self, str):
        for character in str:
            if character in UNICODE_EMOJI:
                return True
        return False

    def do_video_metadata_api_call(self, db_video_meta, video_id):
        try:
            url = self.DATA_VIDEO_API_URL.format(key=self.DATA_API_KEY, id=video_id)
            r = requests.get(url)
            data = r.json()
            if r.status_code != 200:
                logger.info("problem with api call for video {}".format(video_id))
                return
            i = data['items'][0]
            db_video_meta.description = i['snippet'].get('description')
            keywords = i['snippet'].get('tags')
            if keywords:
                db_video_meta.keywords = ','.join(keywords)
            category_id = i['snippet'].get('categoryId')
            if category_id:
                if not category_id in self.categories:
                    self.categories[category_id], _ = AuditCategory.objects.get_or_create(category=category_id)
            db_video_meta.category = self.categories[category_id]
            try:
                db_video_meta.views = int(i['statistics']['viewCount'])
            except Exception as e:
                pass
            try:
                db_video_meta.likes = int(i['statistics']['likeCount'])
            except Exception as e:
                pass
            try:
                db_video_meta.dislikes = int(i['statistics']['dislikeCount'])
            except Exception as e:
                pass
            db_video_meta.emoji = self.audit_video_meta_for_emoji(db_video_meta)
            str_long = db_video_meta.name
            if db_video_meta.keywords:
                str_long = "{} {}".format(str_long, db_video_meta.keywords)
            if db_video_meta.description:
                str_long = "{} {}".format(str_long, db_video_meta.description)
            db_video_meta.language = self.calc_language(str_long)
        except Exception as e:
            logger.exception(e)

    def calc_language(self, data):
        try:
            l = langid.classify(data).lower()
            db_lang, _ = AuditLanguage.objects.get_or_create(language=l)
            return db_lang
        except Exception as e:
            pass

    def do_channel_metadata_api_call(self, db_channel_meta, channel_id):
        try:
            url = self.DATA_CHANNEL_API_URL.format(key=self.DATA_API_KEY, id=channel_id)
            r = requests.get(url)
            data = r.json()
            if r.status_code != 200:
                logger.info("problem with api call for channel {}".format(channel_id))
                return
            i = data['items'][0]
            try:
                db_channel_meta.description = i['brandingSettings']['channel']['description']
            except Exception as e:
                pass
            try:
                db_channel_meta.keywords = i['brandingSettings']['channel']['keywords']
            except Exception as e:
                pass
            try:
                country = i['brandingSettings']['channel']['country']
                if country:
                    db_channel_meta.country. _ = AuditCountry.objects.get_or_create(country=country)
            except Exception as e:
                pass
            db_channel_meta.subscribers = int(i['statistics']['subscriberCount'])
            db_channel_meta.emoji = self.audit_channel_meta_for_emoji(db_channel_meta)
        except Exception as e:
            logger.exception(e)

    def load_inclusion_list(self):
        if self.inclusion_list:
            return
        input_list = self.audit.params.get("inclusion")
        if not input_list:
            return
        regexp = "({})".format(
                "|".join([r"\b{}\b".format(re.escape(w)) for w in input_list])
        )
        self.inclusion_list = re.compile(regexp)

    def load_exclusion_list(self):
        if self.exclusion_list:
            return
        input_list = self.audit.params.get("exclusion")
        if not input_list:
            return
        regexp = "({})".format(
                "|".join([r"\b{}\b".format(re.escape(w)) for w in input_list])
        )
        self.exclusion_list = re.compile(regexp)

    def check_exists(self, text, exp):
        keywords = re.findall(exp, text.lower())
        if len(keywords) > 0:
            return True
        return False

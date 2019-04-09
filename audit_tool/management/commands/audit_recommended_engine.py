import csv
import logging
from django.conf import settings
import re
import requests
from django.utils import timezone
from dateutil.parser import parse
from emoji import UNICODE_EMOJI
from audit_tool.models import AuditCategory
from audit_tool.models import AuditChannel
from audit_tool.models import AuditChannelMeta
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

    def get_current_audit_to_process(self):
        try:
            self.audit = AuditProcessor.objects.filter(completed__isnull=True).order_by("id")[0]
        except Exception as e:
            logger.log("No active audits found {}" .format(e.message))
        self.process_audit()

    def process_audit(self):
        if self.audit.params.get("inclusion"):
            self.load_inclusion_list(self.audit.params.get("inclusion"))
        if self.audit.params.get("exclusion"):
            self.load_exclusion_list(self.audit.params.get("exclusion"))
        pending_videos = AuditVideoProcessor.objects.filter(audit=self.audit)
        if pending_videos.count() == 0:
            pending_videos = self.process_seed_list()
        else:
            pending_videos = pending_videos.filter(processed__isnull=True).order_by("id")
            if pending_videos.count() == 0: # we've processed ALL of the items so we close the audit
                self.audit.completed = timezone.now()
                self.audit.save()
        for video in pending_videos:
            self.do_recommended_api_call(video)

    def process_seed_list(self):
        seed_list = self.audit.params.get('videos')
        vids = []
        for seed in seed_list:
            video = AuditVideo.get_or_create(seed_list.split("/")[-1])
            avp, _ = AuditVideoProcessor.objects.get_or_create(
                audit=self.audit,
                video=video,
                approved=True,
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
            if not db_video.keywords:
                self.do_video_metadata_api_call(db_video_meta, db_video.id)
            db_video.channel = AuditChannel.get_or_create(i['snippet']['channelId'])
            db_video_meta.save()
            db_video.save()
            db_channel_meta, _ = AuditChannelMeta.objects.get_or_create(
                    channel=db_video.channel,
            )
            db_channel_meta.name = i['snippet']['channelTitle']
            db_channel_meta.save()
            # add to this audit IF it passes white/blacklist requirements
            db_avp = AuditVideoProcessor.objects.get_or_create(
                video=db_video,
                audit=self.audit,
                video_source=video,
            )
        avp.processed = timezone.now()
        avp.save(update_fields=['processed'])

    def audit_video_meta_for_emoji(self, db_video_meta):
        if db_video_meta.name and self.contains_emoji(db_video_meta.name):
            return True
        if db_video_meta.description and self.contains_emoji(db_video_meta.description):
            return True
        if db_video_meta.keywords and self.contains_emoji(db_video_meta.keywords):
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
                logger.log("error retrieving {}  from YT".format(video_id))
                return
            i = data['items'][0]
            db_video_meta.description = i['snippet'].get('description')
            db_video_meta.keywords = i['snippet'].get('tags')
            category_id = i['snippet'].get('categoryId')
            if category_id:
                if not category_id in self.categories:
                    self.categories[category_id], _ = AuditCategory.objects.get_or_create(category=category_id)
            db_video_meta.category = self.categories[category_id]
            db_video_meta.views = int(i['statistics']['viewCount'])
            db_video_meta.likes = int(i['statistics']['likeCount'])
            db_video_meta.dislikes = int(i['statistics']['dislikeCount'])
            db_video_meta.emoji = self.audit_video_meta_for_emoji(db_video_meta)
        except Exception as e:
            logger.log("do_video_metadata_api_call: {}".format(e.message))

    def load_inclusion_list(self, input_list):
        regexp = "({})".format(
                "|".join([r"\b{}\b".format(re.escape(w)) for w in input_list])
        )
        self.inclusion_list = re.compile(regexp)

    def load_exclusion_list(self, input_list):
        regexp = "({})".format(
                "|".join([r"\b{}\b".format(re.escape(w)) for w in input_list])
        )
        self.exclusion_list = re.compile(regexp)

    def check_exists(self, text, exp):
        keywords = re.findall(exp, text.lower())
        if len(keywords) > 0: # we found 1 or more matches
            return True
        return False

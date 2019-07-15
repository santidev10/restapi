from django.core.management.base import BaseCommand
import csv
import logging
import re
import requests
from django.utils import timezone
from utils.lang import fasttext_lang
from dateutil.parser import parse
from emoji import UNICODE_EMOJI
from audit_tool.models import AuditCategory
from audit_tool.models import AuditChannel
from audit_tool.models import AuditChannelMeta
from audit_tool.models import AuditExporter
from audit_tool.models import AuditLanguage
from audit_tool.models import AuditProcessor
from audit_tool.models import AuditVideo
from audit_tool.models import AuditVideoMeta
from audit_tool.models import AuditVideoProcessor
from datetime import datetime
logger = logging.getLogger(__name__)
from pid import PidFile
from utils.lang import remove_mentions_hashes_urls
from audit_tool.api.views.audit_save import AuditFileS3Exporter
from django.conf import settings

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

class Command(BaseCommand):
    MAX_VIDS = 1000000
    keywords = []
    inclusion_list = None
    exclusion_list = None
    categories = {}
    audit = None
    DATA_API_KEY = settings.YOUTUBE_API_DEVELOPER_KEY
    DATA_RECOMMENDED_API_URL = "https://www.googleapis.com/youtube/v3/search" \
                               "?key={key}&part=id,snippet&relatedToVideoId={id}" \
                               "&type=video&maxResults=50&relevanceLanguage={language}"
    DATA_VIDEO_API_URL =    "https://www.googleapis.com/youtube/v3/videos" \
                            "?key={key}&part=id,snippet,statistics&id={id}"
    DATA_CHANNEL_API_URL = "https://www.googleapis.com/youtube/v3/channels" \
                         "?key={key}&part=id,statistics,brandingSettings&id={id}"
    CATEGORY_API_URL = "https://www.googleapis.com/youtube/v3/videoCategories" \
                           "?key={key}&part=id,snippet&id={id}"

    def add_arguments(self, parser):
        parser.add_argument('thread_id', type=int)

    # this is the primary method to call to trigger the entire audit sequence
    def handle(self, *args, **options):
        self.thread_id = options.get('thread_id')
        if not self.thread_id:
            self.thread_id = 0
        with PidFile(piddir='.', pidname='get_current_audit_to_process_{}.pid'.format(self.thread_id)) as p:
            try:
                self.audit = AuditProcessor.objects.filter(completed__isnull=True, audit_type=0).order_by("pause", "id")[int(self.thread_id/3)]
                self.language = self.audit.params.get('language')
                if not self.language:
                    self.language = ["en"]
                else:
                    self.language = self.language.split(",")
                self.location = self.audit.params.get('location')
                self.location_radius = self.audit.params.get('location_radius')
                self.category = self.audit.params.get('category')
                self.related_audits = self.audit.params.get('related_audits')
                self.min_date = self.audit.params.get('min_date')
                if self.min_date:
                    self.min_date = datetime.strptime(self.min_date, "%m/%d/%Y")
                self.min_views = self.audit.params.get('min_views')
                self.min_likes = self.audit.params.get('min_likes')
                self.max_dislikes = self.audit.params.get('max_dislikes')
            except Exception as e:
                logger.exception(e)
                raise Exception("no audits to process at present")
            self.process_audit()

    def process_audit(self):
        self.load_inclusion_list()
        self.load_exclusion_list()
        if not self.audit.started:
            self.audit.started = timezone.now()
            self.audit.save(update_fields=['started'])
        pending_videos = AuditVideoProcessor.objects.filter(audit=self.audit)
        thread_id = self.thread_id
        if thread_id % 3 == 0:
            thread_id = 0
        if pending_videos.count() == 0:
            if thread_id == 0:
                pending_videos = self.process_seed_list()
            else:
                raise Exception("waiting for seed list to finish on thread 0")
        else:
            done = False
            if pending_videos.filter(clean=True).count() > self.audit.max_recommended or pending_videos.count() > self.MAX_VIDS:
                done =  True
            pending_videos = pending_videos.filter(processed__isnull=True)
            if pending_videos.count() == 0:  # we've processed ALL of the items so we close the audit
                done =  True
            else:
                pending_videos = pending_videos.select_related("video").order_by("id")
            if done:
                if self.thread_id == 0:
                    self.audit.completed = timezone.now()
                    self.audit.pause = 0
                    self.audit.save(update_fields=['completed', 'pause'])
                    print("Audit completed, all videos processed")
                    a = AuditExporter.objects.create(
                        audit=self.audit,
                        owner=None,
                        clean=True,
                    )
                    raise Exception("Audit completed, all videos processed")
                else:
                    raise Exception("not first thread but audit is done")
        start = thread_id * 100
        for video in pending_videos[start:start+100]:
            self.do_recommended_api_call(video)
        self.audit.updated = timezone.now()
        self.audit.save(update_fields=['updated'])

    def process_seed_file(self, seed_file):
        try:
            f = AuditFileS3Exporter.get_s3_export_csv(seed_file)
        except Exception as e:
            self.audit.params['error'] = "can not open seed file {}".format(seed_file)
            self.audit.completed = timezone.now()
            self.audit.pause = 0
            self.audit.save(update_fields=['params', 'completed', 'pause'])
            raise Exception("can not open seed file {}".format(seed_file))
        reader = csv.reader(f)
        vids = []
        for row in reader:
            avp = self.get_avp_from_url(row[0])
            if avp:
                vids.append(avp)
        if len(vids) == 0:
            self.audit.params['error'] = "no valid YouTube Video URL's in seed file"
            self.audit.completed = timezone.now()
            self.audit.pause = 0
            self.audit.save(update_fields=['params', 'completed', 'pause'])
            raise Exception("no valid YouTube Video URL's in seed file {}".format(seed_file))
        return vids

    def process_seed_list(self):
        seed_list = self.audit.params.get('videos')
        if not seed_list:
            seed_file = self.audit.params.get('seed_file')
            if seed_file:
                return self.process_seed_file(seed_file)
            self.audit.params['error'] = "seed list is empty"
            self.audit.completed = timezone.now()
            self.audit.pause = 0
            self.audit.save(update_fields=['params', 'completed', 'pause'])
            raise Exception("seed list is empty for this audit. {}".format(self.audit.id))
        vids = []
        for seed in seed_list:
            avp = self.get_avp_from_url(seed)
            if avp:
                vids.append(avp)
        return vids

    def get_avp_from_url(self, seed):
        if 'youtube.com' not in seed or ('?v=' not in seed and '/v/' not in seed):
            return
        v_id = seed.replace(",", "").split("/")[-1]
        if '?v=' in v_id:
            v_id = v_id.split("v=")[-1]
        if '?t=' in v_id:
            v_id = v_id.split("?t")[0]
        if v_id:
            v_id = v_id.strip()
            video = AuditVideo.get_or_create(v_id)
            avp, _ = AuditVideoProcessor.objects.get_or_create(
                    audit=self.audit,
                    video=video,
            )
            return avp

    def do_recommended_api_call(self, avp):
        video = avp.video
        if video.video_id is None:
            avp.clean = False
            avp.processed = timezone.now()
            avp.save()
            return
        url = self.DATA_RECOMMENDED_API_URL.format(
            key=self.DATA_API_KEY,
            id=video.video_id,
            language=self.language,
            location="&location={}".format(self.location) if self.location else '',
            location_radius="&locationRadius={}mi".format(self.location_radius) if self.location_radius else ''
        )
        r = requests.get(url)
        data = r.json()
        if 'error' in data:
            if data['error']['message'] == 'Invalid video.':
                avp.processed = timezone.now()
                avp.clean = False
                avp.save()
                return
        for i in data['items']:
            db_video = AuditVideo.get_or_create(i['id']['videoId'])
            db_video_meta, _ = AuditVideoMeta.objects.get_or_create(video=db_video)
            db_video_meta.name = i['snippet']['title']
            db_video_meta.description = i['snippet']['description']
            try:
                db_video_meta.publish_date = parse(i['snippet']['publishedAt'])
            except Exception as e:
                print("no video publish date")
                pass
            if not db_video_meta.keywords:
                self.do_video_metadata_api_call(db_video_meta, db_video.video_id)
            channel = AuditChannel.get_or_create(i['snippet']['channelId'])
            db_video.channel = channel
            db_video_meta.save()
            db_video.save()
            db_channel_meta, _ = AuditChannelMeta.objects.get_or_create(channel=channel)
            if not db_channel_meta.name or db_channel_meta.name != i['snippet']['channelTitle']:
                db_channel_meta.name = i['snippet']['channelTitle']
                db_channel_meta.save(update_fields=['name'])
            is_clean, hits = self.check_video_is_clean(db_video_meta)
            if is_clean:
                if self.check_video_matches_criteria(db_video_meta, db_video):
                    v, _ = AuditVideoProcessor.objects.get_or_create(
                        video=db_video,
                        audit=self.audit
                    )
                    v.word_hits = hits
                    if not v.video_source:
                        v.video_source = video
                    v.clean = self.check_video_matches_minimums(db_video_meta)
                    v.save()

        avp.processed = timezone.now()
        avp.save()

    def check_video_matches_criteria(self, db_video_meta, db_video):
        if self.language:
            if db_video_meta.language and db_video_meta.language.language not in self.language:
                return False
        if self.category:
            if int(db_video_meta.category.category) not in self.category:
                return False
        if self.related_audits:
            if AuditVideoProcessor.objects.filter(video_id=db_video.id, audit_id__in=self.related_audits).exists():
                return False
        return True

    def check_video_matches_minimums(self, db_video_meta):
        if self.min_views:
            if db_video_meta.views < self.min_views:
                return False
        if self.min_date:
            if db_video_meta.publish_date.replace(tzinfo=None) < self.min_date:
                return False
        if self.min_likes:
            if db_video_meta.likes < self.min_likes:
                return False
        if self.max_dislikes:
            if db_video_meta.dislikes > self.max_dislikes:
                return False
        return True

    def check_video_is_clean(self, db_video_meta):
        hits = {}
        full_string = "{} {} {}".format(
            '' if not db_video_meta.name else db_video_meta.name,
            '' if not db_video_meta.description else db_video_meta.description,
            '' if not db_video_meta.keywords else db_video_meta.keywords,
        )
        if self.inclusion_list:
            is_there, b_hits = self.check_exists(full_string, self.inclusion_list)
            hits['inclusion'] = b_hits
            if not is_there:
                return False, hits
        if self.exclusion_list:
            is_there, b_hits = self.check_exists(full_string, self.exclusion_list)
            if is_there:
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
            try:
                i = data['items'][0]
            except Exception as e:
                print("problem getting video {}".format(video_id))
                return
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
            data = remove_mentions_hashes_urls(data).lower()
            l = fasttext_lang(data)
            db_lang, _ = AuditLanguage.objects.get_or_create(language=l)
            return db_lang
        except Exception as e:
            pass

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
            return True, keywords
        return False, None

    def get_categories(self):
        categories = AuditCategory.objects.filter(category_display__isnull=True).values_list('category', flat=True)
        url = self.CATEGORY_API_URL.format(key=self.DATA_API_KEY, id=','.join(categories))
        r = requests.get(url)
        data = r.json()
        for i in data['items']:
            AuditCategory.objects.filter(category=i['id']).update(category_display=i['snippet']['title'])

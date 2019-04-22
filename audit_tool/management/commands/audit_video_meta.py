from django.core.management.base import BaseCommand
import csv
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
from audit_tool.models import AuditLanguage
from audit_tool.models import AuditProcessor
from audit_tool.models import AuditVideo
from audit_tool.models import AuditVideoMeta
from audit_tool.models import AuditVideoProcessor
logger = logging.getLogger(__name__)
from pid.decorator import pidfile

"""
requirements:
    we receive a list of video URLs.
process:
    we go through the videos, grab the meta video data and meta channel
    data, check for blacklist (or whitelist if included) and end with a
    clean list of videos.
"""

class Command(BaseCommand):
    keywords = []
    inclusion_list = None
    exclusion_list = None
    categories = {}
    audit = None
    DATA_API_KEY = settings.YOUTUBE_API_DEVELOPER_KEY
    DATA_VIDEO_API_URL =    "https://www.googleapis.com/youtube/v3/videos" \
                            "?key={key}&part=id,snippet,statistics&id={id}"
    DATA_CHANNEL_API_URL = "https://www.googleapis.com/youtube/v3/channels" \
                         "?key={key}&part=id,statistics,brandingSettings&id={id}"
    CATEGORY_API_URL = "https://www.googleapis.com/youtube/v3/videoCategories" \
                           "?key={key}&part=id,snippet&id={id}"

    # this is the primary method to call to trigger the entire audit sequence
    @pidfile(piddir=".", pidname="audit_video_meta.pid")
    def handle(self, *args, **options):
        try:
            self.audit = AuditProcessor.objects.filter(completed__isnull=True, audit_type=1).order_by("id")[0]
        except Exception as e:
            logger.exception(e)
        self.process_audit()

    def process_audit(self, num=50000):
        self.load_inclusion_list()
        self.load_exclusion_list()
        pending_videos = AuditVideoProcessor.objects.filter(audit=self.audit)
        if pending_videos.count() == 0:
            self.process_seed_list()
            pending_videos = AuditVideoProcessor.objects.filter(
                audit=self.audit,
                processed__isnull=True
            )
        else:
            pending_videos = pending_videos.filter(processed__isnull=True)
        if pending_videos.count() == 0:  # we've processed ALL of the items so we close the audit
            self.audit.completed = timezone.now()
            self.audit.save()
            print("Audit completed, all videos processed")
            raise Exception("Audit completed, all videos processed")
        videos = {}
        pending_videos = pending_videos.select_related("video")
        for video in pending_videos[:num]:
            videos[video.video.video_id] = video
            if len(videos) == 50:
                self.do_check_video(videos)
                videos = {}
        if len(videos) > 0:
            self.do_check_video(videos)
        self.audit.updated = timezone.now()
        self.audit.save()
        print("Done one step, continuing audit {}.".format(self.audit.id))
        raise Exception("Audit completed 1 step.  pausing {}".format(self.audit.id))

    def process_seed_file(self, seed_file):
        with open(seed_file) as f:
            reader = csv.reader(f)
            vids = []
            for row in reader:
                seed = row[0]
                if 'youtube.' in seed:
                    v_id = seed.split("/")[-1]
                    if '?v=' in v_id:
                        v_id = v_id.split("v=")[-1]
                    video = AuditVideo.get_or_create(v_id)
                    avp, _ = AuditVideoProcessor.objects.get_or_create(
                            audit=self.audit,
                            video=video,
                    )
                    vids.append(avp)
            return vids

    def process_seed_list(self):
        seed_list = self.audit.params.get('videos')
        if not seed_list:
            seed_file = self.audit.params.get('seed_file')
            if seed_file:
                return self.process_seed_file(seed_file)
            raise Exception("seed list is empty for this audit. {}".format(self.audit.id))
        vids = []
        for seed in seed_list:
            if 'youtube.' in seed:
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

    def do_check_video(self, videos):
        for video_id, avp in videos.items():
            db_video = avp.video
            db_video_meta, _ = AuditVideoMeta.objects.get_or_create(video=db_video)
            if not db_video_meta.name or not db_video.channel:
                channel_id = self.do_video_metadata_api_call(db_video_meta, video_id)
            else:
                channel_id = db_video.channel.channel_id
            if not channel_id: # video does not exist or is private now
                avp.clean = False
                avp.processed = timezone.now()
                avp.save(update_fields=['processed', 'clean'])
                return
            db_video.channel = AuditChannel.get_or_create(channel_id)
            db_video_meta.save()
            db_video.save()
            db_channel_meta, _ = AuditChannelMeta.objects.get_or_create(
                    channel=db_video.channel,
            )
            avp.clean = self.check_video_is_clean(db_video_meta, avp)
            avp.processed = timezone.now()
            avp.save()

    def check_video_is_clean(self, db_video_meta, avp):
        full_string = "{} {} {}".format(
            '' if not db_video_meta.name else db_video_meta.name,
            '' if not db_video_meta.description else db_video_meta.description,
            '' if not db_video_meta.keywords else db_video_meta.keywords,
        )
        if self.inclusion_list:
            is_there, hits = self.check_exists(full_string, self.inclusion_list)
            avp.word_hits['inclusion'] = hits
            if not is_there:
                return False
        if self.exclusion_list:
            is_there, hits = self.check_exists(full_string, self.exclusion_list)
            avp.word_hits['exclusion'] = hits
            if is_there:
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
            try:
                total = data['pageInfo']['totalResults']
                if total == 0:
                    return None
                else:
                    i = data['items'][0]
            except Exception as e:
                print("problem getting video {}".format(video_id))
                return
            db_video_meta.name = i['snippet']['title']
            db_video_meta.description = i['snippet']['description']
            try:
                db_video_meta.publish_date = parse(i['snippet']['publishedAt'])
            except Exception as e:
                print("no video publish date")
                pass
            db_video_meta.description = i['snippet'].get('description')
            channel_id = i['snippet']['channelId']
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
            return channel_id
        except Exception as e:
            logger.exception(e)

    def calc_language(self, data):
        try:
            l = langid.classify(data.lower())[0]
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

    def export_videos(self, audit_id=None, num_out=None, clean=True):
        self.get_categories()
        cols = [
            "video ID",
            "name",
            "keywords",
            "language",
            "category",
            "views",
            "likes",
            "dislikes",
            "emoji",
            "publish date",
            "channel name",
            "channel ID",
            "country",
            "all hit words",
            "unique hit words",
        ]
        if not audit_id and self.audit:
            audit_id = self.audit.id
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
        if num_out:
            video_meta = video_meta[:num_out]
        with open('export_{}.csv'.format(audit_id), 'w', newline='') as myfile:
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
                all_hit_words, unique_hit_words = self.get_hit_words(hit_words, v.video.video_id)
                data = [
                    v.video.video_id,
                    v.name,
                    v.keywords,
                    language,
                    category,
                    v.views,
                    v.likes,
                    v.dislikes,
                    str(v.emoji),
                    v.publish_date.strftime("%m/%d/%Y, %H:%M:%S") if v.publish_date else '',
                    v.video.channel.auditchannelmeta.name if v.video.channel else  '',
                    v.video.channel.channel_id if v.video.channel else  '',
                    country,
                    all_hit_words,
                    unique_hit_words,
                ]
                wr.writerow(data)

    def get_hit_words(self, hit_words, v_id):
        hits = hit_words.get(v_id)
        uniques = []
        if hits:
            if hits.get('exclusion'):
                for word in hits['exclusion']:
                    if word not in uniques:
                        uniques.append(word)
                return len(hits['exclusion']), ','.join(uniques)
        return '', ''

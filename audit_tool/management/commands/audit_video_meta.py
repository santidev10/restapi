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
from audit_tool.models import AuditChannelProcessor
from audit_tool.models import AuditExporter
from audit_tool.models import AuditLanguage
from audit_tool.models import AuditProcessor
from audit_tool.models import AuditVideo
from audit_tool.models import AuditVideoMeta
from audit_tool.models import AuditVideoProcessor
logger = logging.getLogger(__name__)
from pid import PidFile
from audit_tool.api.views.audit_save import AuditFileS3Exporter
from django.conf import settings
from utils.lang import remove_mentions_hashes_urls

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
    acps = {}
    DATA_API_KEY = settings.YOUTUBE_API_DEVELOPER_KEY
    DATA_VIDEO_API_URL =    "https://www.googleapis.com/youtube/v3/videos" \
                            "?key={key}&part=id,snippet,statistics,contentDetails&id={id}"
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
        try:
            self.machine_number = settings.AUDIT_MACHINE_NUMBER
        except Exception as e:
            self.machine_number = 0
        with PidFile(piddir='.', pidname='audit_video_meta_{}.pid'.format(self.thread_id)) as p:
            #self.check_thread_limit_reached()
            try:
                self.audit = AuditProcessor.objects.filter(temp_stop=False, completed__isnull=True, audit_type=1).order_by("pause", "id")[self.machine_number]
            except Exception as e:
                logger.exception(e)
                raise Exception("no audits to process at present")
            self.process_audit()

    def check_thread_limit_reached(self):
        if self.thread_id > 6:
            if AuditProcessor.objects.filter(audit_type=0, completed__isnull=True).count() > self.machine_number:
                raise Exception("Can not run more video processors while recommendation engine is running")

    def process_audit(self, num=500):
        self.load_inclusion_list()
        self.load_exclusion_list()
        if not self.audit.started:
            self.audit.started = timezone.now()
            self.audit.save(update_fields=['started'])
        pending_videos = AuditVideoProcessor.objects.filter(audit=self.audit)
        if pending_videos.count() == 0:
            if self.thread_id == 0:
                self.process_seed_list()
                pending_videos = AuditVideoProcessor.objects.filter(
                    audit=self.audit,
                    processed__isnull=True
                )
            else:
                raise Exception("waiting to process seed list on thread 0")
        else:
            pending_videos = pending_videos.filter(processed__isnull=True)
        if pending_videos.count() == 0:  # we've processed ALL of the items so we close the audit
            if self.thread_id == 0:
                self.audit.completed = timezone.now()
                self.audit.pause = 0
                self.audit.save(update_fields=['completed', 'pause'])
                print("Audit completed, all videos processed")
                if self.audit.params.get('audit_type_original'):
                    if self.audit.params['audit_type_original'] == 2:
                        self.audit.audit_type = 2
                        self.audit.save(update_fields=['audit_type'])
                a = AuditExporter.objects.create(
                    audit=self.audit,
                    owner_id=None
                )
                raise Exception("Audit completed, all videos processed")
            else:
                raise Exception("not first thread but audit is done")
        videos = {}
        start = self.thread_id * num
        for video in pending_videos[start:start+num]:
            videos[video.video.video_id] = video
            if len(videos) == 50:
                self.do_check_video(videos)
                videos = {}
        if len(videos) > 0:
            self.do_check_video(videos)
        self.audit.updated = timezone.now()
        self.audit.save(update_fields=['updated'])
        print("Done one step, continuing audit {}.".format(self.audit.id))
        raise Exception("Audit {}.  thread {}".format(self.audit.id, self.thread_id))

    def process_seed_file(self, seed_file):
        try:
            f = AuditFileS3Exporter.get_s3_export_csv(seed_file)
        except Exception as e:
            self.audit.params['error'] = "can not open seed file"
            self.audit.completed = timezone.now()
            self.audit.pause = 0
            self.audit.save(update_fields=['params', 'completed', 'pause'])
            raise Exception("can not open seed file {}".format(seed_file))
        reader = csv.reader(f)
        vids = []
        for row in reader:
            seed = row[0]
            if 'youtube.' in seed:
                v_id = seed.strip().split("/")[-1]
                if '?v=' in v_id:
                    v_id = v_id.split("v=")[-1]
                if v_id:
                    video = AuditVideo.get_or_create(v_id)
                    avp, _ = AuditVideoProcessor.objects.get_or_create(
                            audit=self.audit,
                            video=video,
                    )
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
            if not db_video_meta.name or not db_video.channel or not db_video_meta.duration:
                channel_id = self.do_video_metadata_api_call(db_video_meta, video_id)
            else:
                channel_id = db_video.channel.channel_id
            if not channel_id: # video does not exist or is private now
                avp.clean = False
                avp.processed = timezone.now()
                avp.save(update_fields=['processed', 'clean'])
            else:
                db_video.channel = AuditChannel.get_or_create(channel_id)
                db_video_meta.save()
                db_video.save()
                db_channel_meta, _ = AuditChannelMeta.objects.get_or_create(
                        channel=db_video.channel,
                )
                if db_video_meta.publish_date and (not db_channel_meta.last_uploaded or db_channel_meta.last_uploaded < db_video_meta.publish_date):
                    db_channel_meta.last_uploaded = db_video_meta.publish_date
                    db_channel_meta.last_uploaded_view_count = db_video_meta.views
                    db_channel_meta.last_uploaded_category = db_video_meta.category
                    db_channel_meta.save(update_fields=['last_uploaded', 'last_uploaded_view_count', 'last_uploaded_category'])
                avp.clean = self.check_video_is_clean(db_video_meta, avp)
                avp.processed = timezone.now()
                avp.channel = db_video.channel
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
            else:
                self.append_to_channel(avp, hits, 'inclusion_videos')
        if self.exclusion_list:
            is_there, hits = self.check_exists(full_string, self.exclusion_list)
            avp.word_hits['exclusion'] = hits
            if is_there:
                self.append_to_channel(avp, hits, 'exclusion_videos')
                return False
        return True

    def append_to_channel(self, avp, hits, node):
        if self.audit.params['audit_type_original'] == 1:
            return
        channel_id = avp.video.channel_id
        if str(channel_id) not in self.acps:
            try:
                self.acps[str(channel_id)] = AuditChannelProcessor.objects.get(
                    audit_id=avp.audit_id,
                    channel_id=channel_id,
                )
            except Exception as e:
                return
        acp = self.acps[str(channel_id)]
        if node not in acp.word_hits:
            acp.word_hits[node] = []
        for word in hits:
            if word not in acp.word_hits[node]:
                acp.word_hits[node].append(word)
        acp.save(update_fields=['word_hits'])

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
            if 'defaultAudioLanguage' in i['snippet']:
                try:
                    db_video_meta.default_audio_language = AuditLanguage.from_string(i['snippet']['defaultAudioLanguage'])
                except Exception as e:
                    pass
            try:
                db_video_meta.duration = i['contentDetails']['duration']
            except Exception as e:
                pass
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
            data = remove_mentions_hashes_urls(data).lower()
            l = fasttext_lang(data)
            db_lang, _ = AuditLanguage.objects.get_or_create(language=l)
            return db_lang
        except Exception as e:
            pass

    def load_inclusion_list(self):
        if self.inclusion_list:
            return
        input_list = self.audit.params.get("inclusion") if self.audit.params else None
        if not input_list:
            return
        regexp = "({})".format(
                "|".join([r"\b{}\b".format(re.escape(w)) for w in input_list])
        )
        self.inclusion_list = re.compile(regexp)

    def load_exclusion_list(self):
        if self.exclusion_list:
            return
        input_list = self.audit.params.get("exclusion") if self.audit.params else None
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

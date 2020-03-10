import csv
import logging
import re
import requests
from audit_tool.api.views.audit_save import AuditFileS3Exporter
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
from collections import defaultdict
from datetime import timedelta
from dateutil.parser import parse
from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils import timezone
from emoji import UNICODE_EMOJI
from pid import PidFile
from utils.lang import remove_mentions_hashes_urls
from utils.lang import fasttext_lang
from utils.utils import remove_tags_punctuation
from audit_tool.utils.audit_utils import AuditUtils

logger = logging.getLogger(__name__)
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
    MAX_SOURCE_VIDEOS = 750000
    categories = {}
    audit = None
    acps = {}
    num_clones = 0
    original_audit_name = None
    DATA_API_KEY = settings.YOUTUBE_API_DEVELOPER_KEY
    DATA_VIDEO_API_URL =    "https://www.googleapis.com/youtube/v3/videos" \
                            "?key={key}&part=id,status,snippet,statistics,contentDetails&id={id}"
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
                self.audit = AuditProcessor.objects.filter(temp_stop=False, completed__isnull=True, audit_type=1, source=0).order_by("pause", "id")[self.machine_number]
            except Exception as e:
                logger.exception(e)
                raise Exception("no audits to process at present")
            self.process_audit()

    def check_thread_limit_reached(self):
        if self.thread_id > 6:
            if AuditProcessor.objects.filter(audit_type=0, completed__isnull=True).count() > self.machine_number:
                raise Exception("Can not run more video processors while recommendation engine is running")

    def process_audit(self, num=2000):
        self.load_inclusion_list()
        self.load_exclusion_list()
        if not self.audit.started:
            self.audit.started = timezone.now()
            self.audit.save(update_fields=['started'])
        self.exclusion_hit_count = self.audit.params.get('exclusion_hit_count')
        self.inclusion_hit_count = self.audit.params.get('inclusion_hit_count')
        self.db_languages = {}
        self.placement_list = False
        if self.audit.name:
            if 'campaign analysis' in self.audit.name.lower() or 'campaign audit' in self.audit.name.lower():
                self.placement_list = True
        if not self.exclusion_hit_count:
            self.exclusion_hit_count = 1
        else:
            self.exclusion_hit_count = int(self.exclusion_hit_count)
        if not self.inclusion_hit_count:
            self.inclusion_hit_count = 1
        else:
            self.inclusion_hit_count = int(self.inclusion_hit_count)
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
        counter = 0
        for row in reader:
            seed = row[0]
            if 'youtube.' in seed:
                #if seed[-1] == '/':
                #    seed = seed[:-1]
                v_id = seed.strip().split("/")[-1]
                if '?v=' in v_id:
                    v_id = v_id.split("v=")[-1]
                if v_id and len(v_id) < 51:
                    video = AuditVideo.get_or_create(v_id)
                    avp, _ = AuditVideoProcessor.objects.get_or_create(
                            audit=self.audit,
                            video=video,
                    )
                    vids.append(avp)
                    counter+=1
            if len(vids) >= self.MAX_SOURCE_VIDEOS:
                self.clone_audit()
                vids = []
        if counter == 0:
            self.audit.params['error'] = "no valid YouTube Video URL's in seed file"
            self.audit.completed = timezone.now()
            self.audit.pause = 0
            self.audit.save(update_fields=['params', 'completed', 'pause'])
            raise Exception("no valid YouTube Video URL's in seed file {}".format(seed_file))
        return vids

    def clone_audit(self):
        self.num_clones+=1
        if not self.original_audit_name:
            self.original_audit_name = self.audit.params['name']
        self.audit = AuditUtils.clone_audit(self.audit, self.num_clones, name=self.original_audit_name)

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
            if not db_video.processed_time or db_video.processed_time < (timezone.now() - timedelta(days=30)):
                channel_id = self.do_video_metadata_api_call(db_video_meta, video_id)
                db_video.processed_time = timezone.now()
                db_video.save(update_fields=['processed_time'])
            else:
                channel_id = db_video.channel.channel_id if db_video.channel else None
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
                if self.placement_list and not db_channel_meta.monetised:
                    db_channel_meta.monetised = True
                    db_channel_meta.save(update_fields=['monetised'])
                if db_video_meta.publish_date and (not db_channel_meta.last_uploaded or db_channel_meta.last_uploaded < db_video_meta.publish_date):
                    db_channel_meta.last_uploaded = db_video_meta.publish_date
                    db_channel_meta.last_uploaded_view_count = db_video_meta.views
                    db_channel_meta.last_uploaded_category = db_video_meta.category
                    db_channel_meta.save(update_fields=['last_uploaded', 'last_uploaded_view_count', 'last_uploaded_category'])
                avp.channel = db_video.channel
                avp.clean = self.check_video_is_clean(db_video_meta, avp)
                avp.processed = timezone.now()
                avp.save()

    def check_video_is_clean(self, db_video_meta, avp):
        full_string = remove_tags_punctuation("{} {} {}".format(
            '' if not db_video_meta.name else db_video_meta.name,
            '' if not db_video_meta.description else db_video_meta.description,
            '' if not db_video_meta.keywords else db_video_meta.keywords,
        ))
        if self.audit.params.get('do_videos'):
            self.append_to_channel(avp, [avp.video_id], 'processed_video_ids')
        if db_video_meta.made_for_kids == True:
            self.append_to_channel(avp, [avp.video_id], 'made_for_kids')
        if db_video_meta.age_restricted == True:
            avp.word_hits['exclusion'] = ['ytAgeRestricted']
            self.append_to_channel(avp, [avp.video_id], 'bad_video_ids')
            return False
        if self.inclusion_list:
            is_there, hits = self.check_exists(full_string.lower(), self.inclusion_list, count=self.inclusion_hit_count)
            avp.word_hits['inclusion'] = hits
            if not is_there:
                return False
            else:
                self.append_to_channel(avp, hits, 'inclusion_videos')
        if self.exclusion_list:
            try:
                language = db_video_meta.language.language.lower()
            except Exception as e:
                language = ""
            if language not in self.exclusion_list and "" not in self.exclusion_list:
                avp.word_hits['exclusion'] = None
                return True
            is_there = False
            hits = []
            if self.exclusion_list.get(language):
                is_there, hits = self.check_exists(full_string.lower(), self.exclusion_list[language], count=self.exclusion_hit_count)
            if language != "" and self.exclusion_list.get(""):
                is_there_b, b_hits_b = self.check_exists(full_string.lower(), self.exclusion_list[""], count=self.exclusion_hit_count)
                if not is_there and is_there_b:
                    is_there = True
                    hits = b_hits_b
                elif hits and b_hits_b:
                    hits = hits + b_hits_b
            avp.word_hits['exclusion'] = hits
            if is_there:
                self.append_to_channel(avp, [avp.video_id], 'bad_video_ids')
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
            try:
                db_video_meta.made_for_kids = i['status']['madeForKids']
            except Exception as e:
                pass
            if 'defaultAudioLanguage' in i['snippet']:
                try:
                    lang = i['snippet']['defaultAudioLanguage']
                    if lang not in self.db_languages:
                        self.db_languages[lang] = AuditLanguage.from_string(lang)
                    db_video_meta.default_audio_language = self.db_languages[lang]
                except Exception as e:
                    pass
            try:
                db_video_meta.duration = i['contentDetails']['duration']
            except Exception as e:
                pass
            try:
                if i['contentDetails']['contentRating']['ytRating'] == "ytAgeRestricted":
                    db_video_meta.age_restricted = True
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
            if l not in self.db_languages:
                self.db_languages[l] = AuditLanguage.from_string(l)
            return self.db_languages[l]
        except Exception as e:
            pass

    def load_inclusion_list(self):
        if self.inclusion_list:
            return
        input_list = self.audit.params.get("inclusion") if self.audit.params else None
        if not input_list:
            return
        regexp = "({})".format(
                "|".join([r"\b{}\b".format(re.escape(remove_tags_punctuation(w.lower()))) for w in input_list])
        )
        self.inclusion_list = re.compile(regexp)

    def load_exclusion_list(self):
        if self.exclusion_list:
            return
        input_list = self.audit.params.get("exclusion") if self.audit.params else None
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
            except Exception as e:
                language = ""
            language_keywords_dict[language].append(word)
        for lang, keywords in language_keywords_dict.items():
            lang_regexp = "({})".format(
                "|".join([r"\b{}\b".format(re.escape(w.lower())) for w in keywords])
            )
            exclusion_list[lang] = re.compile(lang_regexp)
        self.exclusion_list = exclusion_list

    def check_exists(self, text, exp, count=1):
        keywords = re.findall(exp, remove_tags_punctuation(text.lower()))
        if len(keywords) >= count:
            return True, keywords
        return False, None

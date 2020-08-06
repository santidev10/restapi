import csv
import logging
import re
from collections import defaultdict
from datetime import timedelta
from urllib.parse import urlencode

import requests
from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils import timezone
from pid import PidFile

from audit_tool.api.views.audit_save import AuditFileS3Exporter
from audit_tool.models import AuditChannel
from audit_tool.models import AuditChannelMeta
from audit_tool.models import AuditChannelProcessor
from audit_tool.models import AuditProcessor
from audit_tool.models import AuditVideo
from audit_tool.models import AuditVideoProcessor
from audit_tool.utils.audit_utils import AuditUtils
from utils.utils import remove_tags_punctuation

logger = logging.getLogger(__name__)

"""
requirements:
    we receive a list of channel URLs.
process:
    we go through the channels, create channel objects, let the fill channels
    script get all the channel meta data, and we grab all the videos for each channel.
    once ALL the channels meta data has been grabbed we process for blacklist/whitelist
    then we convert the audit_type from 2 (channels) to 1 (videos) to process each
    of the videos for the audit.  Once all videos are processed the audit is complete.
"""


# pylint: disable=too-many-instance-attributes
class Command(BaseCommand):
    keywords = []
    inclusion_list = None
    exclusion_list = None
    max_pages = 200
    MAX_SOURCE_CHANNELS = 100000
    MAX_SOURCE_CHANNELS_CAP = 300000
    MAX_EMPTY_PLAYLIST_PAGES = 3
    audit = None
    num_clones = 0
    original_audit_name = None
    DATA_API_KEY = settings.YOUTUBE_API_DEVELOPER_KEY
    DATA_CHANNEL_VIDEOS_API_URL = "https://www.googleapis.com/youtube/v3/search" \
                                  "?key={key}&part=id&channelId={id}&order=date{page_token}" \
                                  "&maxResults={num_videos}&type=video"
    CONVERT_USERNAME_API_URL = "https://www.googleapis.com/youtube/v3/channels" \
                               "?key={key}&forUsername={username}&part=id"
    YOUTUBE_CHANNELS_URL = 'https://www.googleapis.com/youtube/v3/channels'
    YOUTUBE_PLAYLISTITEMS_URL = 'https://www.googleapis.com/youtube/v3/playlistItems'
    CHANNEL_VIDEOS_ENDPOINT_MAX_VIDEOS = 500

    def __init__(self, stdout=None, stderr=None, no_color=False, force_color=False):
        super(Command, self).__init__(stdout=stdout, stderr=stderr, no_color=no_color, force_color=force_color)
        self.thread_id = None
        self.machine_number = None
        self.inclusion_hit_count = None
        self.exclusion_hit_count = None
        self.num_videos = None
        self.placement_list = None

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
        try:
            with PidFile(piddir=".", pidname="audit_channel_meta_{}.pid".format(self.thread_id)):
                try:
                    self.audit = AuditProcessor.objects.filter(temp_stop=False, completed__isnull=True, audit_type=2,
                                                               source=0).order_by("pause", "id")[self.machine_number]
                # pylint: disable=broad-except
                except Exception as e:
                # pylint: enable=broad-except
                    logger.exception(e)
                    raise Exception("no audits to process at present")
                self.process_audit()
        # pylint: disable=broad-except
        except Exception as e:
        # pylint: enable=broad-except
            print("problem {} {}".format(self.thread_id, str(e)))

    # pylint: disable=too-many-branches,too-many-statements
    def process_audit(self, num=1000):
        self.load_inclusion_list()
        self.load_exclusion_list()
        self.force_data_refresh = self.audit.params.get("force_data_refresh")
        self.exclusion_hit_count = self.audit.params.get("exclusion_hit_count")
        self.inclusion_hit_count = self.audit.params.get("inclusion_hit_count")
        if not self.exclusion_hit_count:
            self.exclusion_hit_count = 1
        else:
            self.exclusion_hit_count = int(self.exclusion_hit_count)
        if not self.inclusion_hit_count:
            self.inclusion_hit_count = 1
        else:
            self.inclusion_hit_count = int(self.inclusion_hit_count)
        self.num_videos = self.audit.params.get("num_videos")
        self.placement_list = False
        if self.audit.name:
            if "campaign analysis" in self.audit.name.lower() or "campaign audit" in self.audit.name.lower():
                self.placement_list = True
        if not self.num_videos:
            self.num_videos = 50
        if not self.audit.started:
            self.audit.started = timezone.now()
            self.audit.save(update_fields=["started"])
        pending_channels = AuditChannelProcessor.objects.filter(audit=self.audit)
        if not self.audit.params.get("done_source_list") and pending_channels.count() < self.MAX_SOURCE_CHANNELS_CAP:
            if self.thread_id == 0:
                self.process_seed_list()
                if self.num_clones > 0:
                    raise Exception("Done processing seed list, split audit into {} parts".format(self.num_clones + 1))
                pending_channels = AuditChannelProcessor.objects.filter(
                    audit=self.audit,
                    processed__isnull=True
                )
            else:
                raise Exception("waiting to process seed list on thread 0")
        else:
            pending_channels = pending_channels.filter(processed__isnull=True)
        if pending_channels.count() == 0:  # we've processed ALL of the items so we close the audit
            if self.thread_id == 0:
                # if self.audit.params.get("do_videos") == True:
                self.audit.audit_type = 1
                self.audit.params["audit_type_original"] = 2
                self.audit.save(update_fields=["audit_type", "params"])
                print("Audit of channels completed, turning to video processor.")
                raise Exception("Audit of channels completed, turning to video processor")
            raise Exception("not first thread but audit is done")
        pending_channels = pending_channels.filter(channel__processed_time__isnull=False)
        start = self.thread_id * num
        counter = 0
        for channel in pending_channels[start:start + num]:
            counter += 1
            self.do_check_channel(channel)
        self.audit.updated = timezone.now()
        self.audit.save(update_fields=["updated"])
        print("Done one step, continuing audit {}.".format(self.audit.id))
        raise Exception(
            "Audit completed 1 step.  pausing {}. {}.  COUNT: {}".format(self.audit.id, self.thread_id, counter))
    # pylint: enable=too-many-branches,too-many-statements

    # pylint: disable=too-many-statements
    def process_seed_file(self, seed_file):
        try:
            f = AuditFileS3Exporter.get_s3_export_csv(seed_file)
        # pylint: disable=broad-except
        except Exception:
        # pylint: enable=broad-except
            self.audit.params["error"] = "can not open seed file"
            self.audit.completed = timezone.now()
            self.audit.pause = 0
            self.audit.save(update_fields=["params", "completed", "pause"])
            raise Exception("can not open seed file {}".format(seed_file))
        reader = csv.reader(f)
        vids = []
        processed_ids = []
        resume_val = AuditChannelProcessor.objects.filter(audit=self.audit).count()
        print("processing seed file starting at position {}".format(resume_val))
        skipper = 0
        if resume_val > 0:
            for _ in reader:
                if skipper >= resume_val:
                    break
                skipper += 1
        counter = 0
        for row in reader:
            seed = row[0]
            v_id = self.get_channel_id(seed)
            if counter < self.MAX_SOURCE_CHANNELS_CAP and v_id and not v_id in processed_ids:
                processed_ids.append(v_id)
                if len(vids) >= self.MAX_SOURCE_CHANNELS:
                    self.clone_audit()
                    vids = []
                channel = AuditChannel.get_or_create(v_id)
                if channel.processed_time and (self.force_data_refresh or channel.processed_time < timezone.now() - timedelta(days=30)):
                    channel.processed_time = None
                    channel.save(update_fields=["processed_time"])
                AuditChannelMeta.objects.get_or_create(channel=channel)
                acp, _ = AuditChannelProcessor.objects.get_or_create(
                    audit=self.audit,
                    channel=channel,
                )
                vids.append(acp)
                counter += 1
        if counter == 0:
            self.audit.params["error"] = "no valid YouTube Channel URL's in seed file"
            self.audit.completed = timezone.now()
            self.audit.pause = 0
            self.audit.save(update_fields=["params", "completed", "pause"])
            raise Exception("no valid YouTube Channel URL's in seed file {}".format(seed_file))
        audit = self.audit
        audit.params["done_source_list"] = True
        audit.save(update_fields=["params"])
        return vids
    # pylint: enable=too-many-statements

    def clone_audit(self):
        self.num_clones += 1
        if not self.original_audit_name:
            self.original_audit_name = self.audit.params["name"]
        self.audit.params["done_source_list"] = True
        self.audit.save(update_fields=["params"])
        self.audit = AuditUtils.clone_audit(self.audit, self.num_clones, name=self.original_audit_name)

    def get_channel_id(self, seed):
        if "youtube.com/channel/" in seed:
            if seed[-1] == "/":
                seed = seed[:-1]
            v_id = seed.split("/")[-1]
            if "?" in v_id:
                v_id = v_id.split("?")[0]
            return v_id.replace(".", "").replace(";", "")
        if "youtube.com/user/" in seed:
            if seed[-1] == "/":
                seed = seed[:-1]
            username = seed.split("/")[-1]
            url = self.CONVERT_USERNAME_API_URL.format(
                key=self.DATA_API_KEY,
                username=username
            )
            try:
                r = requests.get(url)
                if r.status_code == 200:
                    data = r.json()
                    channel_id = data["items"][0]["id"]
                    return channel_id
            # pylint: disable=broad-except
            except Exception:
            # pylint: enable=broad-except
                pass
        return None

    def process_seed_list(self):
        seed_list = self.audit.params.get("videos")
        if not seed_list:
            seed_file = self.audit.params.get("seed_file")
            if seed_file:
                return self.process_seed_file(seed_file)
            self.audit.params["error"] = "seed list is empty"
            self.audit.completed = timezone.now()
            self.audit.pause = 0
            self.audit.save(update_fields=["params", "completed", "pause"])
            raise Exception("seed list is empty for this audit. {}".format(self.audit.id))
        channels = []
        for seed in seed_list[:self.MAX_SOURCE_CHANNELS]:
            if "youtube.com/channel/" in seed:
                if seed[-1] == "/":
                    seed = seed[:-1]
                v_id = seed.split("/")[-1]
                channel = AuditChannel.get_or_create(v_id)
                AuditChannelMeta.objects.get_or_create(channel=channel)
                avp, _ = AuditChannelProcessor.objects.get_or_create(
                    audit=self.audit,
                    channel=channel,
                )
                channels.append(avp)
        audit = self.audit
        audit.params["done_source_list"] = True
        audit.save(update_fields=["params"])
        return channels

    def do_check_channel(self, acp):
        db_channel = acp.channel
        if db_channel.processed_time:
            db_channel_meta, _ = AuditChannelMeta.objects.get_or_create(channel=db_channel)
            if not acp.processed \
                or acp.processed < (timezone.now() - timedelta(days=7)) \
                or db_channel_meta.last_uploaded < (timezone.now() - timedelta(days=7)):
                self.get_videos(acp)
            acp.processed = timezone.now()
            if db_channel_meta.name:
                acp.clean = self.check_channel_is_clean(db_channel_meta, acp)
            acp.save(update_fields=["clean", "processed", "word_hits"])
            if self.placement_list and not db_channel_meta.monetised:
                db_channel_meta.monetised = True
                db_channel_meta.save(update_fields=["monetised"])

    def handle_bad_response_code(self, response, response_json, acp):
        """
        handles a non-200 response code
        """
        quota_exceed = False
        try:
            if "quota" in response_json["error"]["message"].lower():
                quota_exceed = True
        # pylint: disable=broad-except
        except Exception:
            # pylint: enable=broad-except
            pass
        if quota_exceed:
            logger.info("QUOTA EXCEEDED STOP ASAP!")
            raise Exception("QUOTA EXCEEDED STOP ASAP!")
        logger.info("problem with api call for video %s", acp.channel.channel_id)
        acp.clean = False
        acp.processed = timezone.now()
        acp.word_hits["error"] = response.status_code
        acp.save(update_fields=["clean", "processed", "word_hits"])

    def get_videos_using_uploads_playlist(self, num_videos, acp):
        """
        page through channel's uploads playlist. This is used if a channel
        has more than 500 uploads, since the videos endpoint only gets up
        to 500 video records
        """
        # we want upload playlist id from this response
        channels_url = self.YOUTUBE_CHANNELS_URL + '?' + urlencode({
            'key': self.DATA_API_KEY,
            'id': acp.channel.channel_id,
            'part': ','.join(['contentDetails',]),
        })
        channels_res = requests.get(channels_url)
        channels_json = channels_res.json()
        if channels_res.status_code != 200:
            self.handle_bad_response_code(channels_res, channels_json, acp)
            return
        items = channels_json.get('items', [])
        if not len(items):
            logger.info("could not get channel playlists for channel with id %s", acp.channel.channel_id)
            self.handle_bad_response_code(channels_res, channels_json, acp)
            return
        channel_json = items[0]
        uploads_playlist_id = channel_json['contentDetails']['relatedPlaylists']['uploads']
        # page through uploads playlist and collect video ids
        count = 0
        previous_page_counts = []
        next_page_token = None
        while True:
            playlist_url_params = {
                'key': self.DATA_API_KEY,
                'playlistId': uploads_playlist_id,
                'part': 'snippet',
                'maxResults': 50,
            }
            if next_page_token:
                playlist_url_params['pageToken'] = next_page_token
            playlist_url = self.YOUTUBE_PLAYLISTITEMS_URL + '?' + urlencode(playlist_url_params)
            playlist_res = requests.get(playlist_url)
            playlist_json = playlist_res.json()
            if playlist_res.status_code != 200:
                self.handle_bad_response_code(playlist_res, playlist_json, acp)
                return
            for item in playlist_json['items']:
                self.update_or_create_video(item['snippet']['resourceId']['videoId'], acp)
                count += 1
            # prevent unnecessary quota usage if res is 200, but no items returned
            previous_page_counts.append(len(playlist_json['items']))
            if len(previous_page_counts) >= self.MAX_EMPTY_PLAYLIST_PAGES \
                and not sum(previous_page_counts[-self.MAX_EMPTY_PLAYLIST_PAGES:]):
                break
            if count >= num_videos:
                break
            next_page_token = playlist_json.get('nextPageToken', None)
            if next_page_token is None:
                break

    def get_videos_using_channel_videos(self, num_videos, acp):
        """
        get a channels' videos. Used if a channel has less than 500 video
        uploads. This only allows paging through the first 500 results
        """
        has_more = True
        page_token = None
        page = 0
        count = 0
        per_page = num_videos
        if per_page > 50:
            per_page = 50
        while has_more:
            page = page + 1
            if page_token:
                pt = "&pageToken={}".format(page_token)
            else:
                pt = ""
            url = self.DATA_CHANNEL_VIDEOS_API_URL.format(
                key=self.DATA_API_KEY,
                id=acp.channel.channel_id,
                page_token=pt,
                num_videos=per_page,
            )
            count += per_page
            r = requests.get(url)
            data = r.json()
            if r.status_code != 200:
                self.handle_bad_response_code(r, data, acp)
                return
            page_token = data.get("nextPageToken")
            if not page_token \
                    or page >= self.max_pages \
                    or not self.audit.params.get("do_videos") \
                    or per_page < 50 \
                    or count >= num_videos:
                has_more = False
            for item in data["items"]:
                self.update_or_create_video(item['id']['videoId'], acp)

    def update_or_create_video(self, video_id, acp):
        """
        create or update AuditVideo and AuditVideoProcessor records,
        given a video id
        """
        db_video = AuditVideo.get_or_create(video_id)
        if not db_video.channel or db_video.channel != acp.channel:
            db_video.channel = acp.channel
            db_video.save(update_fields=["channel"])
        AuditVideoProcessor.objects.get_or_create(
            audit=self.audit,
            video=db_video
        )

    def get_videos(self, acp):
        """
        Updates or Creates a given Audit Channel's videos up to self.num_videos.
        """
        num_videos = self.num_videos
        if not self.audit.params.get("do_videos"):
            num_videos = 1
        try:
            channel_video_count = acp.channel.auditchannelmeta.video_count
        except AuditChannelMeta.DoesNotExist:
            channel_video_count = None
        if channel_video_count and channel_video_count > self.CHANNEL_VIDEOS_ENDPOINT_MAX_VIDEOS and num_videos > self.CHANNEL_VIDEOS_ENDPOINT_MAX_VIDEOS:
            self.get_videos_using_uploads_playlist(num_videos, acp)
            return
        self.get_videos_using_channel_videos(num_videos, acp)

    def load_inclusion_list(self):
        if self.inclusion_list:
            return
        input_list = self.audit.params.get("inclusion") if self.audit.params else None
        if not input_list:
            return
        regexp = "({})".format(
            "|".join([r"\b{}\b".format(re.escape(remove_tags_punctuation(w))) for w in input_list])
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
            # pylint: disable=broad-except
            except Exception:
            # pylint: enable=broad-except
                language = ""
            language_keywords_dict[language].append(word)
        for lang, keywords in language_keywords_dict.items():
            lang_regexp = "({})".format(
                "|".join([r"\b{}\b".format(re.escape(w.lower())) for w in keywords])
            )
            exclusion_list[lang] = re.compile(lang_regexp)
        self.exclusion_list = exclusion_list

    def check_channel_is_clean(self, db_channel_meta, acp):
        full_string = remove_tags_punctuation("{} {} {}".format(
            "" if not db_channel_meta.name else db_channel_meta.name,
            "" if not db_channel_meta.description else db_channel_meta.description,
            "" if not db_channel_meta.keywords else db_channel_meta.keywords,
        ))
        if self.inclusion_list:
            is_there, hits = self.check_exists(full_string.lower(), self.inclusion_list,
                                               count=self.inclusion_hit_count)
            acp.word_hits["inclusion"] = hits
            if not is_there:
                return False
        if self.exclusion_list:
            try:
                language = db_channel_meta.language.language.lower()
            # pylint: disable=broad-except
            except Exception:
            # pylint: enable=broad-except
                language = ""
            if language not in self.exclusion_list and "" not in self.exclusion_list:
                acp.word_hits["exclusion"] = None
                return True
            is_there = False
            hits = []
            if self.exclusion_list.get(language):
                is_there, hits = self.check_exists(full_string.lower(), self.exclusion_list[language],
                                                   count=self.exclusion_hit_count)
            if language != "" and self.exclusion_list.get(""):
                is_there_b, b_hits_b = self.check_exists(full_string.lower(), self.exclusion_list[""],
                                                         count=self.exclusion_hit_count)
                if not is_there and is_there_b:
                    is_there = True
                    hits = b_hits_b
                elif hits and b_hits_b:
                    hits = hits + b_hits_b
            acp.word_hits["exclusion"] = hits
            if is_there:
                return False
        return True

    def check_exists(self, text, exp, count=1):
        keywords = re.findall(exp, remove_tags_punctuation(text))
        if len(keywords) >= count:
            return True, keywords
        return False, None
# pylint: enable=too-many-instance-attributes

import csv
import logging
import re
from collections import defaultdict
from datetime import timedelta
import tempfile
import os

import requests
from dateutil.parser import parse
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db.models import F
from django.utils import timezone
from emoji import UNICODE_EMOJI
from pid import PidFile

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
from audit_tool.models import BlacklistItem
from audit_tool.utils.audit_utils import AuditUtils
from segment.models import CustomSegment
from segment.models.utils.generate_segment_utils import GenerateSegmentUtils
from utils.lang import fasttext_lang
from utils.lang import remove_mentions_hashes_urls
from utils.utils import remove_tags_punctuation
from utils.utils import chunks_generator


logger = logging.getLogger(__name__)
"""
requirements:
    we receive a list of video URLs.
process:
    we go through the videos, grab the meta video data and meta channel
    data, check for blacklist (or whitelist if included) and end with a
    clean list of videos.
"""


# pylint: disable=too-many-instance-attributes
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
        self.inclusion_hit_count = None
        self.exclusion_hit_count = None
        self.db_languages = None
        self.placement_list = None
        self.check_titles = True

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
        with PidFile(piddir=".", pidname="audit_video_meta_{}.pid".format(self.thread_id)):
            # self.check_thread_limit_reached()
            try:
                self.audit = AuditProcessor.objects.filter(temp_stop=False, completed__isnull=True, audit_type=1,
                                                           source__in=[0,2]).order_by("pause", "id")[self.machine_number]
            # pylint: disable=broad-except
            except Exception as e:
            # pylint: enable=broad-except
                logger.exception(e)
                raise Exception("no audits to process at present")
            self.process_audit()

    def check_thread_limit_reached(self):
        if self.thread_id > 6:
            if AuditProcessor.objects.filter(audit_type=0, completed__isnull=True).count() > self.machine_number:
                raise Exception("Can not run more video processors while recommendation engine is running")

    def update_ctl(self):
        """ Create export for CTL using audited data """
        segment = CustomSegment.objects.get(id=self.audit.params["segment_id"])
        if self.audit.audit_type == 1:
            audit_model = AuditVideoProcessor
            model_fk_ref = "video"
            url_separator = "?v="
        elif self.audit.audit_type == 2:
            audit_model = AuditChannelProcessor
            model_fk_ref = "channel"
            url_separator = "/channel/"
        else:
            return
        clean_audits = audit_model.objects \
            .filter(audit=self.audit, clean=True) \
            .select_related(model_fk_ref) \
            .annotate(item_id=F(f"{model_fk_ref}__{model_fk_ref}_id"))
        clean_ids = set(audit.item_id for audit in clean_audits)
        temp_file = tempfile.mkstemp(dir=settings.TEMPDIR, suffix=".csv")[1]
        write_header = True
        try:
            # Get original export file to filter using cleaned audit data
            export_filename = segment.export.filename
            export_fp = segment.s3.download_file(export_filename, f"{settings.TEMPDIR}/{export_filename}")

            with open(export_fp, mode="r") as read_file, \
                    open(temp_file, mode="w", newline="\n") as dest_file:
                reader = csv.reader(read_file)
                writer = csv.writer(dest_file)
                for chunk in chunks_generator(reader, size=2000):
                    chunk = list(chunk)
                    if write_header is True:
                        header = chunk.pop(0)
                        writer.writerow(header)
                        write_header = False
                    rows = [row for row in chunk if row[0].split(url_separator)[-1] in clean_ids]
                    writer.writerows(rows)
            # Replace segment export with the audited file
            segment.s3.export_file_to_s3(temp_file, segment.export.filename)
            aggregations = GenerateSegmentUtils(segment).get_aggregations_by_ids(clean_ids)
            segment.statistics = {
                "items_count": len(clean_ids),
                **aggregations
            }
            segment.save()
        # pylint: disable=broad-except
        except Exception as err:
            logger.exception(err)
        else:
            os.remove(export_fp)
        # pylint: enable=broad-except
        finally:
            os.remove(temp_file)

    # pylint: disable=too-many-branches,too-many-statements
    def process_audit(self, num=2000):
        self.load_inclusion_list()
        self.load_exclusion_list()
        self.force_data_refresh = self.audit.params.get("force_data_refresh")
        if not self.audit.started:
            self.audit.started = timezone.now()
            self.audit.save(update_fields=["started"])
        self.exclusion_hit_count = self.audit.params.get("exclusion_hit_count")
        self.inclusion_hit_count = self.audit.params.get("inclusion_hit_count")
        self.db_languages = {}
        self.placement_list = False
        if self.audit.name:
            if "campaign analysis" in self.audit.name.lower() or "campaign audit" in self.audit.name.lower():
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
        if not self.audit.params.get("done_source_list") and pending_videos.count() < self.MAX_SOURCE_VIDEOS:
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
                self.audit.save(update_fields=["completed", "pause"])
                print("Audit completed, all videos processed")
                if self.audit.params.get("audit_type_original"):
                    if self.audit.params["audit_type_original"] == 2:
                        self.audit.audit_type = 2
                        self.audit.save(update_fields=["audit_type"])
                if self.audit.source == 0:
                    AuditExporter.objects.create(
                        audit=self.audit,
                        owner_id=None
                    )
                elif self.audit.source==2:
                    self.update_ctl()
                raise Exception("Audit completed, all videos processed")
            raise Exception("not first thread but audit is done")
        videos = {}
        start = self.thread_id * num
        for video in pending_videos[start:start + num]:
            videos[video.video.video_id] = video
            if len(videos) == 50:
                self.do_check_video(videos)
                videos = {}
        if len(videos) > 0:
            self.do_check_video(videos)
        self.audit.updated = timezone.now()
        self.audit.save(update_fields=["updated"])
        print("Done one step, continuing audit {}.".format(self.audit.id))
        raise Exception("Audit {}.  thread {}".format(self.audit.id, self.thread_id))
    # pylint: enable=too-many-branches,too-many-statements

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
        counter = 0
        processed_ids = []
        resume_val = AuditVideoProcessor.objects.filter(audit=self.audit).count()
        print("processing seed file starting at position {}".format(resume_val))
        skipper = 0
        if resume_val > 0:
            for _ in reader:
                if skipper >= resume_val:
                    break
                skipper += 1
        for row in reader:
            seed = row[0]
            if "youtube." in seed:
                # if seed[-1] == "/":
                #    seed = seed[:-1]
                v_id = seed.strip().split("/")[-1]
                if "?v=" in v_id:
                    v_id = v_id.split("v=")[-1]
                v_id = v_id.replace(".", "").replace(";", "")
                if v_id and len(v_id) < 51 and not v_id in processed_ids:
                    processed_ids.append(v_id)
                    if len(vids) >= self.MAX_SOURCE_VIDEOS:
                        self.clone_audit()
                        vids = []
                    video = AuditVideo.get_or_create(v_id)
                    avp, _ = AuditVideoProcessor.objects.get_or_create(
                        audit=self.audit,
                        video=video,
                    )
                    vids.append(avp)
                    counter += 1
        if counter == 0 and resume_val == 0:
            self.audit.params["error"] = "no valid YouTube Video URL's in seed file"
            self.audit.completed = timezone.now()
            self.audit.pause = 0
            self.audit.save(update_fields=["params", "completed", "pause"])
            raise Exception("no valid YouTube Video URL's in seed file {}".format(seed_file))
        audit = self.audit
        audit.params["done_source_list"] = True
        audit.save(update_fields=["params"])
        return vids

    def clone_audit(self):
        self.num_clones += 1
        if not self.original_audit_name:
            self.original_audit_name = self.audit.params["name"]
        self.audit.params["done_source_list"] = True
        self.audit.save(update_fields=["params"])
        self.audit = AuditUtils.clone_audit(self.audit, self.num_clones, name=self.original_audit_name)

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
        vids = []
        for seed in seed_list:
            if "youtube." in seed:
                v_id = seed.split("/")[-1]
                if "?v=" in v_id:
                    v_id = v_id.split("v=")[-1]
                video = AuditVideo.get_or_create(v_id)
                avp, _ = AuditVideoProcessor.objects.get_or_create(
                    audit=self.audit,
                    video=video,
                )
                vids.append(avp)
        audit = self.audit
        audit.params["done_source_list"] = True
        audit.save(update_fields=["params"])
        return vids

    def do_check_video(self, videos):
        for video_id, avp in videos.items():
            db_video = avp.video
            db_video_meta, _ = AuditVideoMeta.objects.get_or_create(video=db_video)
            if not db_video.processed_time or self.force_data_refresh or db_video.processed_time < (timezone.now() - timedelta(days=30)):
                channel_id = self.do_video_metadata_api_call(db_video_meta, video_id)
                db_video.processed_time = timezone.now()
                db_video.save(update_fields=["processed_time"])
            else:
                channel_id = db_video.channel.channel_id if db_video.channel else None
            if not channel_id:  # video does not exist or is private now
                avp.clean = False
                avp.processed = timezone.now()
                avp.save(update_fields=["processed", "clean"])
            else:
                db_video.channel = AuditChannel.get_or_create(channel_id)
                db_video_meta.save()
                db_video.save()
                db_channel_meta, _ = AuditChannelMeta.objects.get_or_create(
                    channel=db_video.channel,
                )
                if self.placement_list and not db_channel_meta.monetised:
                    db_channel_meta.monetised = True
                    db_channel_meta.save(update_fields=["monetised"])
                if db_video_meta.publish_date \
                    and (not db_channel_meta.last_uploaded
                         or db_channel_meta.last_uploaded < db_video_meta.publish_date):
                    db_channel_meta.last_uploaded = db_video_meta.publish_date
                    db_channel_meta.last_uploaded_view_count = db_video_meta.views
                    db_channel_meta.last_uploaded_category = db_video_meta.category
                    db_channel_meta.save(
                        update_fields=["last_uploaded", "last_uploaded_view_count", "last_uploaded_category"])
                avp.channel = db_video.channel
                #if not self.audit.params.get("override_blocklist"):
                blocklisted = self.check_video_is_blocklisted(db_video.video_id, channel_id, avp)
                if not blocklisted:
                    avp.clean = self.check_video_is_clean(db_video_meta, avp)
                else:
                    avp.clean = False
                avp.processed = timezone.now()
                avp.save()

    def check_video_is_blocklisted(self, video_id, channel_id, avp):
        if BlacklistItem.get(channel_id, BlacklistItem.CHANNEL_ITEM):
            avp.word_hits["exclusion"] = ['BLOCKLIST']
            #self.append_to_channel(avp, [avp.video_id], "bad_video_ids")
            return True
        if BlacklistItem.get(video_id, BlacklistItem.VIDEO_ITEM):
            avp.word_hits["exclusion"] = ['BLOCKLIST']
            self.append_to_channel(avp, [avp.video_id], "bad_video_ids")
            return True

    def check_video_is_clean(self, db_video_meta, avp):
        title_string = remove_tags_punctuation("" if not db_video_meta.name else db_video_meta.name)
        other_string = remove_tags_punctuation("{} {}".format(
            "" if not db_video_meta.description else db_video_meta.description,
            "" if not db_video_meta.keywords else db_video_meta.keywords,
        ))
        full_string = remove_tags_punctuation("{} {} {}".format(
            "" if not db_video_meta.name else db_video_meta.name,
            "" if not db_video_meta.description else db_video_meta.description,
            "" if not db_video_meta.keywords else db_video_meta.keywords,
        ))
        if self.audit.params.get("do_videos"):
            self.append_to_channel(avp, [avp.video_id], "processed_video_ids")
        if db_video_meta.made_for_kids:
            self.append_to_channel(avp, [avp.video_id], "made_for_kids")
        if db_video_meta.age_restricted:
            avp.word_hits["exclusion"] = ["ytAgeRestricted"]
            self.append_to_channel(avp, [avp.video_id], "age_restricted_videos")
            self.append_to_channel(avp, [avp.video_id], "bad_video_ids")
            return False
        if self.inclusion_list:
            is_there, hits = self.check_exists(full_string.lower(), self.inclusion_list,
                                               count=self.inclusion_hit_count)
            avp.word_hits["inclusion"] = hits
            if not is_there:
                return False
            self.append_to_channel(avp, hits, "inclusion_videos")
        if self.exclusion_list:
            try:
                language = db_video_meta.language.language.lower()
            # pylint: disable=broad-except
            except Exception:
            # pylint: enable=broad-except
                language = ""
            if language not in self.exclusion_list and "" not in self.exclusion_list:
                avp.word_hits["exclusion"] = None
                return True
            is_there = False
            is_there_title = False
            hits = []
            hits_title = []
            if self.check_titles: # separate blacklist words in title
                if self.exclusion_list.get(language):
                    is_there, hits = self.check_exists(other_string.lower(), self.exclusion_list[language],
                                                       count=self.exclusion_hit_count)
                    is_there_title, hits_title = self.check_exists(title_string.lower(), self.exclusion_list[language],
                                                       count=self.exclusion_hit_count)
                if language != "" and self.exclusion_list.get(""):
                    is_there_b, b_hits_b = self.check_exists(other_string.lower(), self.exclusion_list[""],
                                                             count=self.exclusion_hit_count)
                    is_there_b_title, b_hits_b_title = self.check_exists(title_string.lower(), self.exclusion_list[""],
                                                             count=self.exclusion_hit_count)
                    if not is_there and is_there_b:
                        is_there = True
                        hits = b_hits_b
                    elif hits and b_hits_b:
                        hits = hits + b_hits_b

                    if not is_there_title and is_there_b_title:
                        is_there_title = True
                        hits_title = b_hits_b_title
                    elif hits_title and b_hits_b_title:
                        hits_title = hits_title + b_hits_b_title
                avp.word_hits["exclusion"] = hits
                avp.word_hits["exclusion_title"] = hits_title
                if is_there:
                    self.append_to_channel(avp, hits, "exclusion_videos")
                if is_there_title:
                    self.append_to_channel(avp, hits_title, "exclusion_videos_title")
                if is_there or is_there_title:
                    self.append_to_channel(avp, [avp.video_id], "bad_video_ids")
                    return False
            else:
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
                avp.word_hits["exclusion"] = hits
                if is_there:
                    self.append_to_channel(avp, [avp.video_id], "bad_video_ids")
                    self.append_to_channel(avp, hits, "exclusion_videos")
                    return False
        return True

    def append_to_channel(self, avp, hits, node):
        if self.audit.params["audit_type_original"] == 1:
            return
        channel_id = avp.video.channel_id
        if str(channel_id) not in self.acps:
            try:
                self.acps[str(channel_id)] = AuditChannelProcessor.objects.get(
                    audit_id=avp.audit_id,
                    channel_id=channel_id,
                )
            # pylint: disable=broad-except
            except Exception:
            # pylint: enable=broad-except
                return
        acp = self.acps[str(channel_id)]
        if node not in acp.word_hits:
            acp.word_hits[node] = []
        for word in hits:
            if word not in acp.word_hits[node]:
                acp.word_hits[node].append(word)
        acp.save(update_fields=["word_hits"])

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
                return None
            try:
                total = data["pageInfo"]["totalResults"]
                if total == 0:
                    return None
                i = data["items"][0]
            # pylint: disable=broad-except
            except Exception:
            # pylint: enable=broad-except
                print("problem getting video {}".format(video_id))
                return None
            db_video_meta.name = i["snippet"]["title"]
            db_video_meta.description = i["snippet"]["description"]
            try:
                if i["snippet"]["liveBroadcastContent"] in ["live", "upcoming"]:
                    db_video_meta.live_broadcast = True
                else:
                    db_video_meta.live_broadcast = False
            except Exception:
                pass
            try:
                html = i["player"]["embedHtml"]
                width = int(html.split("width=\"")[1].split("\"")[0])
                height = int(html.split("height=\"")[1].split("\"")[0])
                aspect_ratio = round(width / height * 1.0, 2)
                db_video_meta.aspect_ratio = aspect_ratio
            except Exception:
                pass
            try:
                db_video_meta.publish_date = parse(i["snippet"]["publishedAt"])
            # pylint: disable=broad-except
            except Exception:
            # pylint: enable=broad-except
                print("no video publish date")
            db_video_meta.description = i["snippet"].get("description")
            channel_id = i["snippet"]["channelId"]
            keywords = i["snippet"].get("tags")
            if keywords:
                db_video_meta.keywords = " ".join(keywords)
            category_id = i["snippet"].get("categoryId")
            if category_id:
                if not category_id in self.categories:
                    self.categories[category_id], _ = AuditCategory.objects.get_or_create(category=category_id)
            db_video_meta.category = self.categories[category_id]
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
            db_video_meta.emoji = self.audit_video_meta_for_emoji(db_video_meta)
            try:
                db_video_meta.made_for_kids = i["status"]["madeForKids"]
            # pylint: disable=broad-except
            except Exception:
            # pylint: enable=broad-except
                pass
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
            return channel_id
        # pylint: disable=broad-except
        except Exception as e:
        # pylint: enable=broad-except
            logger.exception(e)
        return None
    # pylint: enable=too-many-branches,too-many-statements

    def calc_language(self, data):
        try:
            data = remove_mentions_hashes_urls(data).lower()
            l = fasttext_lang(data)
            if l not in self.db_languages:
                self.db_languages[l] = AuditLanguage.from_string(l)
            return self.db_languages[l]
        # pylint: disable=broad-except
        except Exception:
        # pylint: enable=broad-except
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

    def check_exists(self, text, exp, count=1):
        keywords = re.findall(exp, remove_tags_punctuation(text.lower()))
        if len(keywords) >= count:
            return True, keywords
        return False, None
# pylint: enable=too-many-instance-attributes

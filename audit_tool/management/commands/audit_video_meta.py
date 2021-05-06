import csv
import logging
import os
import re
import requests
import tempfile

from collections import defaultdict
from datetime import timedelta
from dateutil.parser import parse
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db.models import F
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
from audit_tool.models import AuditVideoMeta
from audit_tool.models import AuditVideoProcessor
from audit_tool.models import BlacklistItem
from audit_tool.utils.regex_trie import get_optimized_regex
from segment.models import CustomSegment
from segment.models.constants import Params
from segment.models.utils.generate_segment_utils import GenerateSegmentUtils
from segment.utils.utils import get_content_disposition
from segment.tasks.generate_sdf_segment import generate_sdf_segment_task
from utils.lang import fasttext_lang
from utils.lang import remove_mentions_hashes_urls
from utils.utils import chunks_generator
from utils.utils import remove_tags_punctuation

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
    categories = {}
    audit = None
    acps_data = {}
    NUM_THREADS = settings.AUDIT_VIDEO_NUM_THREADS
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
        with PidFile(piddir="pids", pidname="audit_video_meta_{}.pid".format(self.thread_id)):
            # self.check_thread_limit_reached()
            try:
                self.audit = AuditProcessor.objects.filter(temp_stop=False, seed_status=2, completed__isnull=True, audit_type__in=[1,2],
                                                           source__in=[0,2]).order_by("-source", "pause", "id")[self.machine_number]
            # pylint: disable=broad-except
            except Exception as e:
            # pylint: enable=broad-except
                logger.exception(e)
                raise Exception("no audits to process at present")
            self.process_audit()

    def check_thread_limit_reached(self):
        if self.thread_id > 6:
            if AuditProcessor.objects.filter(seed_status=2, audit_type=0, completed__isnull=True).count() > self.machine_number:
                raise Exception("Can not run more video processors while recommendation engine is running")

    def update_ctl(self):
        """
        Create export for CTL using audited data
        Will start generate_sdf_segment_task if audit was started from SegmentDV360SyncAPIView
        """
        segment = CustomSegment.objects.get(id=self.audit.params["segment_id"])
        if self.audit.audit_type == 1:
            url_separator = "?v="
            clean_audits = AuditVideoProcessor.objects.filter(audit=self.audit, clean=True)\
                .annotate(yt_id=F("video__video_id"))
        elif self.audit.audit_type == 2:
            url_separator = "/channel/"
            # If a channel contains one unclean video, the entire channel is considered unclean
            unclean_channel_ids = AuditVideoProcessor.objects.filter(audit=self.audit, clean=False)\
                .annotate(yt_channel_id=F("channel__channel_id")) \
                .exclude(yt_channel_id__isnull=True) \
                .values_list("yt_channel_id")\
                .distinct()
            clean_audits = AuditChannelProcessor.objects.filter(audit=self.audit)\
                .exclude(channel__channel_id__in=unclean_channel_ids)\
                .annotate(yt_id=F("channel__channel_id"))
        else:
            return
        clean_ids = set(yt_id for yt_id in clean_audits.values_list("yt_id", flat=True))
        temp_file = tempfile.mkstemp(dir=settings.TEMPDIR, suffix=".csv")[1]
        admin_temp_file = tempfile.mkstemp(dir=settings.TEMPDIR, suffix=".csv")[1]
        write_header = True
        try:
            # Get original export files to filter using cleaned audit data
            export_filename = segment.export.filename
            admin_export_filename = segment.export.admin_filename
            export_fp = segment.s3.download_file(export_filename, f"{settings.TEMPDIR}/{export_filename}")
            admin_export_fp = segment.s3.download_file(admin_export_filename,
                                                       f"{settings.TEMPDIR}/{admin_export_filename}")
            quote_char = '"'
            self.write_audit_to_file(export_fp, temp_file, quote_char, url_separator, \
                                     clean_ids, write_header)
            self.write_audit_to_file(admin_export_fp, admin_temp_file, quote_char, \
                                     url_separator, clean_ids, write_header)
            # Replace segment export with the audited file
            content_disposition = get_content_disposition(segment)
            segment.s3.export_file_to_s3(temp_file, segment.export.filename,
                                         extra_args=dict(ContentDisposition=content_disposition))
            segment.s3.export_file_to_s3(admin_temp_file, segment.export.admin_filename,
                                         extra_args=dict(ContentDisposition=content_disposition))
            aggregations = GenerateSegmentUtils(segment).get_aggregations_by_ids(clean_ids)
            segment.statistics = {
                "items_count": len(clean_ids),
                **aggregations
            }
            segment.save(update_fields=["statistics"])
        # pylint: disable=broad-except
        except Exception as err:
            logger.exception(err)
        else:
            os.remove(export_fp)
            os.remove(admin_export_fp)
            dv360_params = self.audit.params.get(Params.DV360_SYNC_DATA)
            if dv360_params:
                generate_sdf_segment_task.delay(self.audit.params["user_id"], segment.id,
                                          dv360_params[Params.ADVERTISER_ID], dv360_params[Params.ADGROUP_IDS])
        # pylint: enable=broad-except
        finally:
            os.remove(temp_file)
            os.remove(admin_temp_file)

    def write_audit_to_file(self, export_fp, temp_file, quote_char, url_separator, clean_ids, write_header=True,
                            membership=True):
        """ rewrites files in s3 according to audit results """
        with open(export_fp, mode="r") as read_file, \
                open(temp_file, mode="w") as dest_file:
            reader = csv.reader(read_file, quotechar=quote_char)
            writer = csv.writer(dest_file, quotechar=quote_char)
            for chunk in chunks_generator(reader, size=2000):
                chunk = list(chunk)
                if write_header is True:
                    header = chunk.pop(0)
                    writer.writerow(header)
                    write_header = False
                rows = [row for row in chunk if row[0].split(url_separator)[-1] in clean_ids]
                writer.writerows(rows)

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
        pending_videos = AuditVideoProcessor.objects.filter(audit=self.audit).filter(processed__isnull=True)
        if pending_videos.count() == 0:  # we've processed ALL of the items so we close the audit
            if self.thread_id == 0:
                if self.audit.params.get("audit_type_original") and self.audit.params["audit_type_original"] == 2:
                    if AuditChannelProcessor.objects.filter(audit=self.audit, processed__isnull=True).exists():
                        raise Exception("videos done, but channels still processing.")
                    else:
                        self.audit.audit_type = 2
                self.audit.completed = timezone.now()
                self.audit.pause = 0
                self.audit.save(update_fields=["completed", "pause", "audit_type"])
                print("Audit completed, all videos processed")
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
        threads = []
        for video in pending_videos[start:start + num]:
            videos[video.video.video_id] = video
            if len(videos) == 50:
                t = Thread(target=self.do_check_video, args=(videos,))
                threads.append(t)
                t.start()
                if len(threads) >= self.NUM_THREADS:
                    for t in threads:
                        t.join()
                    threads = []
                # self.do_check_video(videos)
                videos = {}
        if len(threads) > 0:
            for t in threads:
                t.join()
        if len(videos) > 0:
            self.do_check_video(videos)
        self.audit.updated = timezone.now()
        self.audit.save(update_fields=["updated"])
        self.update_acps_word_hits_from_local_dict()
        print("Done one step, continuing audit {}.".format(self.audit.id))
        raise Exception("Audit {}.  thread {}".format(self.audit.id, self.thread_id))
    # pylint: enable=too-many-branches,too-many-statements

    def update_acps_word_hits_from_local_dict(self):
        for channel_id_str, acp_data in self.acps_data.items():
            if len(acp_data['word_hits']) > 0:
                try:
                    db_acp = AuditChannelProcessor.objects.get(audit_id=acp_data['audit_id'],
                                                               channel_id=acp_data['channel_id'])
                # pylint: disable=broad-except
                except Exception:
                # pylint: enable=broad-except
                    continue
                for node in acp_data['word_hits']:
                    node_items_set = set(acp_data['word_hits'][node])
                    if node in db_acp.word_hits:
                        node_items_set.update(db_acp.word_hits[node])
                    db_acp.word_hits[node] = list(node_items_set)
                db_acp.save(update_fields=['word_hits'])

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
                try:
                    db_video_meta.save()
                except ValueError:
                    db_video_meta.keywords = ''
                    try:
                        db_video_meta.save()
                    except ValueError:
                        db_video_meta.description = ''
                        try:
                            db_video_meta.save()
                        except ValueError:
                            db_video_meta.name = ''
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
        if str(channel_id) not in self.acps_data:
            self.acps_data[str(channel_id)] = {
                    'audit_id': avp.audit_id,
                    'channel_id': channel_id,
                    'word_hits': {}
            }
        if node not in self.acps_data[str(channel_id)]['word_hits']:
            self.acps_data[str(channel_id)]['word_hits'][node] = []
        for word in hits:
            self.acps_data[str(channel_id)]['word_hits'][node].append(word)

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
                if db_video_meta.likes and db_video_meta.likes > 9223372036854775807:
                    db_video_meta.likes = 9223372036854775807
            # pylint: disable=broad-except
            except Exception:
            # pylint: enable=broad-except
                pass
            try:
                db_video_meta.dislikes = int(i["statistics"]["dislikeCount"])
                if db_video_meta.dislikes and db_video_meta.dislikes > 9223372036854775807:
                    db_video_meta.dislikes = 9223372036854775807
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
        self.inclusion_list = get_optimized_regex(words_list=input_list, remove_tags_punctuation_from_words=True)

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
            exclusion_list[lang] = get_optimized_regex(words_list=keywords)
        self.exclusion_list = exclusion_list

    def check_exists(self, text, exp, count=1):
        keywords = re.findall(exp, remove_tags_punctuation(text.lower()))
        if len(keywords) >= count:
            return True, keywords
        return False, None
# pylint: enable=too-many-instance-attributes

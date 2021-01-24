import logging

import requests
from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils import timezone
from emoji import UNICODE_EMOJI
from pid import PidFile
from threading import Thread

from audit_tool.models import AuditChannel
from audit_tool.models import AuditChannelMeta
from audit_tool.models import AuditCountry
from audit_tool.models import AuditLanguage
from audit_tool.models import AuditVideo
from audit_tool.models import AuditVideoMeta
from utils.lang import fasttext_lang
from utils.lang import remove_mentions_hashes_urls
from utils.utils import convert_subscriber_count

logger = logging.getLogger(__name__)

"""
requirements:
    to fill in the DB with channel data where necessary.
process:
    look at AuditChannel objects that haven"t been "processed"
    and go in batch to youtube to get the data to fill in.
"""


class Command(BaseCommand):
    DATA_API_KEY = settings.YOUTUBE_API_DEVELOPER_KEY
    DATA_CHANNEL_API_URL = "https://www.googleapis.com/youtube/v3/channels" \
                           "?key={key}&part=id,statistics,brandingSettings&id={id}"
    cache = {
        "countries": {},
        "languages": {}
    }
    NUM_THREADS = settings.AUDIT_FILL_CHANNEL_DATA_NUM_THREADS
    def __init__(self, stdout=None, stderr=None, no_color=False, force_color=False):
        super(Command, self).__init__(stdout=stdout, stderr=stderr, no_color=no_color, force_color=force_color)
        self.thread_id = None

    def add_arguments(self, parser):
        parser.add_argument("thread_id", type=int)

    def handle(self, *args, **options):
        thread_id = options.get("thread_id")
        if not thread_id:
            thread_id = 0
        with PidFile(piddir=".", pidname="audit_fill_channels{}.pid".format(thread_id)):
            count = 0
            pending_channels = AuditChannelMeta.objects.filter(channel__processed_time__isnull=True)
            total_to_go = pending_channels.count()
            if total_to_go == 0:
                logger.info("No channels to fill.")
                self.fill_recent_video_timestamp()
                raise Exception("No channels to fill.")
            channels = {}
            num = 500
            start = thread_id * num
            threads = []
            for channel in pending_channels[start:start + num]:
                channels[channel.channel.channel_id] = channel
                count += 1
                if len(channels) == 50:
                    t = Thread(target=self.do_channel_metadata_api_call, args=(channels,))
                    threads.append(t)
                    t.start()
                    if len(threads) >= self.NUM_THREADS:
                        for t in threads:
                            t.join()
                        threads = []
                    # self.do_channel_metadata_api_call(channels)
                    channels = {}
            if len(channels) > 0:
                # self.do_channel_metadata_api_call(channels)
                t = Thread(target=self.do_channel_metadata_api_call, args=(channels,))
                threads.append(t)
                t.start()
            if len(threads) > 0:
                for t in threads:
                    t.join()
            logger.info("Done %s channels", count)
            total_pending = total_to_go - count
            if total_pending < 0:
                total_pending = 0
            if thread_id == 0:
                self.fill_recent_video_timestamp()
            raise Exception("Done {} channels: {} total pending.".format(count, total_pending))

    def fill_recent_video_timestamp(self):
        channels = AuditChannelMeta.objects.filter(video_count__gt=0, last_uploaded_view_count__isnull=True).order_by(
            "-id")
        for c in channels[:5000]:
            db_videos = AuditVideo.objects.filter(channel=c.channel).values_list("id", flat=True)
            videos = AuditVideoMeta.objects.filter(video_id__in=db_videos).order_by("-publish_date")
            try:
                c.last_uploaded = videos[0].publish_date
                c.last_uploaded_view_count = videos[0].views
                c.last_uploaded_category = videos[0].category
                c.save(update_fields=["last_uploaded", "last_uploaded_view_count", "last_uploaded_category"])
            # pylint: disable=broad-except
            except Exception:
            # pylint: enable=broad-except
                pass

    def calc_video_languages_and_sentiment(self, channel):
        videos = AuditVideoMeta.objects.filter(video__channel=channel.channel)
        if videos.count() == 0:
            return
        languages = {}
        likes = 0
        dislikes = 0
        for v in videos:
            if v.likes:
                likes+=v.likes
            if v.dislikes:
                dislikes+=v.dislikes
            if v.language:
                language = v.language.language
                if language not in languages:
                    languages[language] = 0
                languages[language] += 1
        channel.likes = likes
        channel.dislikes = dislikes
        if languages and languages != {}:
            try:
                l = sorted(languages.items(), key=lambda x: x[1], reverse=True)[0][0]
                if l not in self.cache["languages"]:
                    self.cache["languages"][l] = AuditLanguage.from_string(l)
                channel.primary_video_language = self.cache["languages"][l]
            except Exception:
                pass
        try:
            channel.save(update_fields=["primary_video_language", "likes", "dislikes"])
        except Exception:
            pass

    def calc_language(self, channel):
        str_long = channel.name
#        if channel.keywords:
#            str_long = "{} {}".format(str_long, channel.keywords)
        if channel.description:
            str_long = "{} {}".format(str_long, channel.description)
        try:
            str_long = remove_mentions_hashes_urls(str_long).lower()
            l = fasttext_lang(str_long)
            if l not in self.cache["languages"]:
                self.cache["languages"][l] = AuditLanguage.from_string(l)
            channel.language = self.cache["languages"][l]
            channel.save(update_fields=["language"])
        # pylint: disable=broad-except
        except Exception:
        # pylint: enable=broad-except
            pass

    # pylint: disable=too-many-branches,too-many-nested-blocks,too-many-statements
    def do_channel_metadata_api_call(self, channels):
        ids = []
        for i, _ in channels.items():
            ids.append(i)
        try:
            url = self.DATA_CHANNEL_API_URL.format(key=self.DATA_API_KEY, id=",".join(ids))
            r = requests.get(url)
            data = r.json()
            if r.status_code != 200:
                logger.info("problem with api call")
                return
            # reset the name of previously found channels
            for c in AuditChannel.objects.filter(processed_time__isnull=False, channel_id__in=ids):
                try:
                    c.auditchannelmeta.name = ""
                    c.auditchannelmeta.save(update_fields=['name'])
                except Exception:
                    pass
            for i in data["items"]:
                try:
                    db_channel_meta = channels[i["id"]]
                # pylint: disable=broad-except
                except Exception:
                # pylint: enable=broad-except
                    db_channel = AuditChannel.get_or_create(i["id"])
                    db_channel_meta, _ = AuditChannelMeta.objects.get_or_create(channel=db_channel)
                if not i.get("brandingSettings"):
                    continue
                try:
                    db_channel_meta.name = i["brandingSettings"]["channel"]["title"]
                # pylint: disable=broad-except
                except Exception:
                # pylint: enable=broad-except
                    pass
                try:
                    db_channel_meta.description = i["brandingSettings"]["channel"]["description"]
                # pylint: disable=broad-except
                except Exception:
                # pylint: enable=broad-except
                    pass
                try:
                    db_channel_meta.keywords = i["brandingSettings"]["channel"]["keywords"]
                # pylint: disable=broad-except
                except Exception:
                # pylint: enable=broad-except
                    pass
                try:
                    if i["brandingSettings"]["channel"]["defaultLanguage"]:
                        if i["brandingSettings"]["channel"]["defaultLanguage"] not in self.cache["languages"]:
                            default_language = i["brandingSettings"]["channel"]["defaultLanguage"]
                            self.cache["languages"][default_language] = AuditLanguage.from_string(
                                in_var=default_language)
                        db_channel_meta.default_language = self.cache["languages"][
                            i["brandingSettings"]["channel"]["defaultLanguage"]]
                # pylint: disable=broad-except
                except Exception:
                # pylint: enable=broad-except
                    pass
                try:
                    country = i["brandingSettings"]["channel"].get("country")
                # pylint: disable=broad-except
                except Exception:
                # pylint: enable=broad-except
                    country = None
                if country:
                    if country not in self.cache["countries"]:
                        self.cache["countries"][country] = AuditCountry.from_string(country)
                    db_channel_meta.country = self.cache["countries"][country]
                db_channel_meta.subscribers = convert_subscriber_count(i["statistics"].get("subscriberCount"))
                if db_channel_meta.subscribers is None:
                    db_channel_meta.subscribers = 0
                try:
                    db_channel_meta.hidden_subscriber_count = i["statistics"]["hiddenSubscriberCount"]
                # pylint: disable=broad-except
                except Exception:
                # pylint: enable=broad-except
                    pass
                try:
                    db_channel_meta.view_count = int(i["statistics"]["viewCount"])
                # pylint: disable=broad-except
                except Exception:
                # pylint: enable=broad-except
                    pass
                try:
                    db_channel_meta.video_count = int(i["statistics"]["videoCount"])
                # pylint: disable=broad-except
                except Exception:
                # pylint: enable=broad-except
                    pass
                db_channel_meta.emoji = self.audit_channel_meta_for_emoji(db_channel_meta)
                try:
                    db_channel_meta.save()
                    self.calc_language(db_channel_meta)
                # pylint: disable=broad-except
                except Exception:
                # pylint: enable=broad-except
                    logger.info("problem saving channel")
                self.calc_video_languages_and_sentiment(db_channel_meta)
            AuditChannel.objects.filter(channel_id__in=ids).update(processed_time=timezone.now())
        # pylint: disable=broad-except
        except Exception as e:
        # pylint: enable=broad-except
            logger.exception(e)

    # pylint: enable=too-many-branches,too-many-nested-blocks,too-many-statements

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

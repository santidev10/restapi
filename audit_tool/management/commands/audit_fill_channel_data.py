from django.core.management.base import BaseCommand
from utils.lang import remove_mentions_hashes_urls
from utils.lang import fasttext_lang
import logging
from django.conf import settings
import requests
from emoji import UNICODE_EMOJI
from audit_tool.models import AuditChannel
from audit_tool.models import AuditChannelMeta
from audit_tool.models import AuditCountry
from audit_tool.models import AuditLanguage
from audit_tool.models import AuditVideo
from audit_tool.models import AuditVideoMeta
from utils.utils import convert_subscriber_count
from django.utils import timezone
logger = logging.getLogger(__name__)
from pid import PidFile

"""
requirements:
    to fill in the DB with channel data where necessary.
process:
    look at AuditChannel objects that haven't been 'processed'
    and go in batch to youtube to get the data to fill in.
"""

class Command(BaseCommand):
    DATA_API_KEY = settings.YOUTUBE_API_DEVELOPER_KEY
    DATA_CHANNEL_API_URL = "https://www.googleapis.com/youtube/v3/channels" \
                         "?key={key}&part=id,statistics,brandingSettings&id={id}"
    cache = {
        'countries': {},
        'languages': {}
    }

    def add_arguments(self, parser):
        parser.add_argument('thread_id', type=int)

    def handle(self, *args, **options):
        self.thread_id = options.get('thread_id')
        if not self.thread_id:
            self.thread_id = 0
        with PidFile(piddir='.', pidname='audit_fill_channels{}.pid'.format(self.thread_id)) as p:
            count = 0
            pending_channels = AuditChannelMeta.objects.filter(channel__processed_time__isnull=True)
            if pending_channels.count() == 0:
                logger.info("No channels to fill.")
                self.fill_recent_video_timestamp()
                raise Exception("No channels to fill.")
            channels = {}
            num = 1000
            start = self.thread_id * num
            total_to_go = pending_channels.count()
            for channel in pending_channels.order_by("-id")[start:start+num]:
                channels[channel.channel.channel_id] = channel
                count+=1
                if len(channels) == 50:
                    self.do_channel_metadata_api_call(channels)
                    channels = {}
            if len(channels) > 0:
                self.do_channel_metadata_api_call(channels)
            logger.info("Done {} channels".format(count))
            total_pending = total_to_go - count
            if total_pending < 0:
                total_pending = 0
            raise Exception("Done {} channels: {} total pending.".format(count, total_pending))

    def fill_recent_video_timestamp(self):
        channels = AuditChannelMeta.objects.filter(video_count__gt=0, last_uploaded_view_count__isnull=True).order_by("-id")
        for c in channels[:5000]:
            db_videos = AuditVideo.objects.filter(channel=c.channel).values_list('id', flat=True)
            videos = AuditVideoMeta.objects.filter(video_id__in=db_videos).order_by("-publish_date")
            try:
                c.last_uploaded = videos[0].publish_date
                c.last_uploaded_view_count = videos[0].views
                c.last_uploaded_category = videos[0].category
                c.save(update_fields=['last_uploaded', 'last_uploaded_view_count', 'last_uploaded_category'])
            except Exception as e:
                pass

    def calc_language(self, channel):
        str_long = channel.name
        if channel.keywords:
            str_long = "{} {}".format(str_long, channel.keywords)
        if channel.description:
            str_long = "{} {}".format(str_long, channel.description)
        try:
            str_long = remove_mentions_hashes_urls(str_long).lower()
            l = fasttext_lang(str_long)
            if l not in self.cache['languages']:
                self.cache['languages'][l], _ = AuditLanguage.objects.get_or_create(language=l)
            channel.language = self.cache['languages'][l]
            channel.save(update_fields=['language'])
        except Exception as e:
            pass

    def do_channel_metadata_api_call(self, channels):
        ids = []
        for i, _ in channels.items():
            ids.append(i)
        try:
            url = self.DATA_CHANNEL_API_URL.format(key=self.DATA_API_KEY, id=','.join(ids))
            r = requests.get(url)
            data = r.json()
            if r.status_code != 200:
                logger.info("problem with api call")
                return
            for i in data['items']:
                db_channel_meta = channels[i['id']]
                if not i.get('brandingSettings'):
                    continue
                try:
                    db_channel_meta.name = i['brandingSettings']['channel']['title']
                except Exception as e:
                    pass
                try:
                    db_channel_meta.description = i['brandingSettings']['channel']['description']
                except Exception as e:
                    pass
                try:
                    db_channel_meta.keywords = i['brandingSettings']['channel']['keywords']
                except Exception as e:
                    pass
                try:
                    if i['brandingSettings']['channel']['defaultLanguage']:
                        if i['brandingSettings']['channel']['defaultLanguage'] not in self.cache['languages']:
                            self.cache['languages'][i['brandingSettings']['channel']['defaultLanguage']], _ = AuditLanguage.objects.get_or_create(language=i['brandingSettings']['channel']['defaultLanguage'])
                        db_channel_meta.default_language = self.cache['languages'][i['brandingSettings']['channel']['defaultLanguage']]
                except Exception as e:
                    pass
                try:
                    country = i['brandingSettings']['channel'].get('country')
                except Exception as e:
                    country = None
                    pass
                if country:
                    if country not in self.cache['countries']:
                        self.cache['countries'][country] = AuditCountry.from_string(country)
                    db_channel_meta.country = self.cache['countries'][country]
                db_channel_meta.subscribers = convert_subscriber_count(i['statistics']['subscriberCount'])
                try:
                    db_channel_meta.view_count = int(i['statistics']['viewCount'])
                except Exception as e:
                    pass
                try:
                    db_channel_meta.video_count = int(i['statistics']['videoCount'])
                except Exception as e:
                    pass
                db_channel_meta.emoji = self.audit_channel_meta_for_emoji(db_channel_meta)
                try:
                    db_channel_meta.save()
                    self.calc_language((db_channel_meta))
                except Exception as e:
                    logger.info("problem saving channel")
            AuditChannel.objects.filter(channel_id__in=ids).update(processed_time=timezone.now())
        except Exception as e:
            logger.exception(e)

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
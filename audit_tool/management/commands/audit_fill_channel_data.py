from django.core.management.base import BaseCommand
import logging
from django.conf import settings
import requests
from emoji import UNICODE_EMOJI
from audit_tool.models import AuditChannel
from audit_tool.models import AuditChannelMeta
from audit_tool.models import AuditCountry
from audit_tool.models import AuditLanguage
logger = logging.getLogger(__name__)
from pid.decorator import pidfile

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

    @pidfile(piddir=".", pidname="audit_fill_channels.pid")
    def handle(self, *args, **options):
        count = 0
        pending_channels = AuditChannelMeta.objects.filter(channel__processed=False).select_related("channel")
        if pending_channels.count() == 0:
            logger.info("No channels to fill.")
            raise Exception("No channels to fill.")
        channels = {}
        for channel in pending_channels[:20000]:
            channels[channel.channel.channel_id] = channel
            count+=1
            if len(channels) == 50:
                self.do_channel_metadata_api_call(channels)
                channels = {}
        if len(channels) > 0:
            self.do_channel_metadata_api_call(channels)
        logger.info("Done {} channels".format(count))
        raise Exception("Done {} channels".format(count))

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
                    db_lang, _ = AuditLanguage.objects.get_or_create(language=i['brandingSettings']['channel']['defaultLanguage'])
                    db_channel_meta.default_language = db_lang
                except Exception as e:
                    pass
                country = i['brandingSettings']['channel'].get('country')
                if country:
                    db_channel_meta.country, _ = AuditCountry.objects.get_or_create(country=country)
                db_channel_meta.subscribers = int(i['statistics']['subscriberCount'])
                try:
                    db_channel_meta.view_count = int(i['statistics']['viewCount'])
                except Exception as e:
                    pass
                db_channel_meta.emoji = self.audit_channel_meta_for_emoji(db_channel_meta)
                db_channel_meta.save()
            AuditChannel.objects.filter(channel_id__in=ids).update(processed=True)
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
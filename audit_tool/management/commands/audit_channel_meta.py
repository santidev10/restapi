from django.core.management.base import BaseCommand
import csv
import logging
from django.conf import settings
import re
import requests
from django.utils import timezone
from audit_tool.models import AuditChannel
from audit_tool.models import AuditChannelMeta
from audit_tool.models import AuditChannelProcessor
from audit_tool.models import AuditProcessor
from audit_tool.models import AuditVideo
from audit_tool.models import AuditVideoProcessor
logger = logging.getLogger(__name__)
from pid.decorator import pidfile
from audit_tool.api.views.audit_save import AuditFileS3Exporter

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

class Command(BaseCommand):
    keywords = []
    inclusion_list = None
    exclusion_list = None
    audit = None
    DATA_API_KEY = settings.YOUTUBE_API_DEVELOPER_KEY
    DATA_CHANNEL_VIDEOS_API_URL = "https://www.googleapis.com/youtube/v3/search" \
                            "?key={key}&part=id&channelId={id}&order=viewCount{page_token}" \
                            "&maxResults=50&type=video"

    # this is the primary method to call to trigger the entire audit sequence
    @pidfile(piddir=".", pidname="audit_channel_meta.pid")
    def handle(self, *args, **options):
        try:
            self.audit = AuditProcessor.objects.filter(completed__isnull=True, audit_type=2).order_by("pause", "id")[0]
        except Exception as e:
            logger.exception(e)
        self.process_audit()

    def process_audit(self, num=50000):
        self.load_inclusion_list()
        self.load_exclusion_list()
        if not self.audit.started:
            self.audit.started = timezone.now()
            self.audit.save(update_fields=['started'])
        pending_channels = AuditChannelProcessor.objects.filter(audit=self.audit)
        if pending_channels.count() == 0:
            self.process_seed_list()
            pending_channels = AuditChannelProcessor.objects.filter(
                audit=self.audit,
                processed__isnull=True
            )
        else:
            pending_channels = pending_channels.filter(processed__isnull=True)
        if pending_channels.count() == 0:  # we've processed ALL of the items so we close the audit
            if self.audit.params.get('do_videos') == True:
                self.audit.audit_type = 1
                self.audit.params['audit_type_original'] = 2
                self.audit.save(update_fields=['audit_type', 'params'])
                print("Audit of channels completed, turning to video processor.")
                raise Exception("Audit of channels completed, turning to video processor")
            else:
                self.audit.completed = timezone.now()
                self.audit.save(update_fields=['completed'])
                print("Audit of channels completed, turning to video processor.")
                raise Exception("Audit of channels completed, turning to video processor")
        pending_channels = pending_channels.filter(channel__processed=True).select_related("channel")
        for channel in pending_channels[:num]:
            self.do_check_channel(channel)
        self.audit.updated = timezone.now()
        self.audit.save(update_fields=['updated'])
        print("Done one step, continuing audit {}.".format(self.audit.id))
        raise Exception("Audit completed 1 step.  pausing {}".format(self.audit.id))

    def process_seed_file(self, seed_file):
        f = AuditFileS3Exporter.get_s3_export_csv(seed_file)
        reader = csv.reader(f)
        vids = []
        for row in reader:
            seed = row[0]
            if 'youtube.com/channel/' in seed:
                v_id = seed.split("/")[-1]
                channel = AuditChannel.get_or_create(v_id)
                AuditChannelMeta.objects.get_or_create(channel=channel)
                acp, _ = AuditChannelProcessor.objects.get_or_create(
                        audit=self.audit,
                        channel=channel,
                )
                vids.append(acp)
        return vids
        self.audit.params['error'] = "can not open seed file {}".format(seed_file)
        self.audit.completed = timezone.now()
        self.audit.save(update_fields=['params', 'completed'])
        raise Exception("can not open seed file {}".format(seed_file))

    def process_seed_list(self):
        seed_list = self.audit.params.get('videos')
        if not seed_list:
            seed_file = self.audit.params.get('seed_file')
            if seed_file:
                return self.process_seed_file(seed_file)
            self.audit.params['error'] = "seed list is empty"
            self.audit.completed = timezone.now()
            self.audit.save(update_fields=['params', 'completed'])
            raise Exception("seed list is empty for this audit. {}".format(self.audit.id))
        channels = []
        for seed in seed_list:
            if 'youtube.com/channel/' in seed:
                if seed[-1] == '/':
                    seed = seed[:-1]
                v_id = seed.split("/")[-1]
                channel = AuditChannel.get_or_create(v_id)
                AuditChannelMeta.objects.get_or_create(channel=channel)
                avp, _ = AuditChannelProcessor.objects.get_or_create(
                    audit=self.audit,
                    channel=channel,
                )
                channels.append(avp)
        return channels

    def do_check_channel(self, acp):
        db_channel = acp.channel
        db_channel_meta, _ = AuditChannelMeta.objects.get_or_create(channel=db_channel)
        self.get_videos(acp)
        acp.processed = timezone.now()
        if db_channel_meta.name:
            acp.clean = self.check_channel_is_clean(db_channel_meta, acp)
        acp.save(update_fields=['clean', 'processed'])

    def get_videos(self, acp):
        db_channel = acp.channel
        has_more = True
        page_token = None
        while has_more:
            if page_token:
                pt = "&pageToken={}".format(page_token)
            else:
                pt=''
            url = self.DATA_CHANNEL_VIDEOS_API_URL.format(
                key=self.DATA_API_KEY,
                id=db_channel.channel_id,
                page_token=pt
            )
            r = requests.get(url)
            data = r.json()
            if r.status_code != 200:
                logger.info("problem with api call for video {}".format(db_channel.channel_id))
                acp.clean = False
                acp.processed = timezone.now()
                acp.save(update_fields=['clean', 'processed'])
                return
            page_token = data.get('nextPageToken')
            if not page_token:
                has_more = False
            for item in data['items']:
                db_video = AuditVideo.get_or_create(item['id']['videoId'])
                db_video.channel = db_channel
                db_video.save(update_fields=['channel'])
                AuditVideoProcessor.objects.get_or_create(
                    audit=self.audit,
                    video=db_video
                )

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

    def check_channel_is_clean(self, db_channel_meta, acp):
        full_string = "{} {} {}".format(
            '' if not db_channel_meta.name else db_channel_meta.name,
            '' if not db_channel_meta.description else db_channel_meta.description,
            '' if not db_channel_meta.keywords else db_channel_meta.keywords,
        )
        if self.inclusion_list:
            is_there, hits = self.check_exists(full_string, self.inclusion_list)
            acp.word_hits['inclusion'] = hits
            if not is_there:
                return False
        if self.exclusion_list:
            is_there, hits = self.check_exists(full_string, self.exclusion_list)
            acp.word_hits['exclusion'] = hits
            if is_there:
                return False
        return True

    def check_exists(self, text, exp):
        keywords = re.findall(exp, text.lower())
        if len(keywords) > 0:
            return True, keywords
        return False, None

    def export_channels(self, audit_id=None):
        cols = [
            "Channel Title",
            "Channel ID",
            "views",
            "subscribers",
            "num_videos",
            "country",
            "language",
            "unique bad words",
            "bad words",
        ]
        if not audit_id and self.audit:
            audit_id = self.audit.id
        channel_ids = []
        hit_words = {}
        video_count = {}
        channels = AuditChannelProcessor.objects.filter(audit_id=audit_id).select_related("channel")
        for cid in channels:
            channel_ids.append(cid.channel_id)
            hit_words[cid.channel.channel_id] = cid.word_hits.get('exclusion')
            if not hit_words[cid.channel.channel_id]:
                hit_words[cid.channel.channel_id] = []
            videos = AuditVideoProcessor.objects.filter(audit_id=audit_id, video__channel_id=cid.channel_id)
            video_count[cid.channel.channel_id] = videos.count()
            for video in videos:
                if video.word_hits.get('exclusion'):
                    for bad_word in video.word_hits.get('exclusion'):
                        if bad_word not in hit_words[cid.channel.channel_id]:
                            hit_words[cid.channel.channel_id].append(bad_word)
        channel_meta = AuditChannelMeta.objects.filter(channel_id__in=channel_ids).select_related(
                "channel",
                "language",
                "country"
        )
        try:
            name = self.audit.params['name'].replace("/", "-")
        except Exception as e:
            name = audit_id
        with open('export_{}.csv'.format(name), 'w', newline='') as myfile:
            wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
            wr.writerow(cols)
            for v in channel_meta:
                try:
                    language = v.language.language
                except Exception as e:
                    language = ""
                try:
                    country = v.country.country
                except Exception as e:
                    country = ""
                data = [
                    v.name,
                    v.channel.channel_id,
                    v.view_count,
                    v.subscribers,
                    video_count[v.channel.channel_id],
                    country,
                    language,
                    len(hit_words[v.channel.channel_id]),
                    ','.join(hit_words[v.channel.channel_id])
                ]
                wr.writerow(data)
            if self.audit and self.audit.completed:
                self.audit.params['export'] = 'export_{}.csv'.format(name)
                self.audit.save()
            return 'export_{}.csv'.format(name)
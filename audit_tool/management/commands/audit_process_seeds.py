import csv
import logging
from time import sleep
import requests

from datetime import timedelta
from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils import timezone
from pid import PidFile

from audit_tool.api.views.audit_save import AuditFileS3Exporter
from audit_tool.models import AuditChannel
from audit_tool.models import AuditChannelMeta
from audit_tool.models import AuditChannelProcessor
from audit_tool.models import AuditProcessor
from audit_tool.models import AuditVideoProcessor
from audit_tool.utils.audit_utils import AuditUtils

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    MAX_SOURCE_VIDEOS = 750000
    MAX_SOURCE_CHANNELS = 100000
    MAX_SOURCE_CHANNELS_CAP = 300000
    num_clones = 0
    original_audit_name = None
    CONVERT_USERNAME_API_URL = "https://www.googleapis.com/youtube/v3/channels" \
                               "?key={key}&forUsername={username}&part=id"
    DATA_API_KEY = settings.YOUTUBE_API_DEVELOPER_KEY

    def __init__(self, stdout=None, stderr=None, no_color=False, force_color=False):
        super(Command, self).__init__(stdout=stdout, stderr=stderr, no_color=no_color, force_color=force_color)
        self.thread_id = None
        self.machine_number = None
        self.audit = None

    def add_arguments(self, parser):
        parser.add_argument("thread_id", type=int)

    def handle(self, *args, **options):
        self.thread_id = options.get("thread_id")
        if not self.thread_id:
            self.thread_id = 0
        with PidFile(piddir=".", pidname="process_seed_{}.pid".format(self.thread_id)):
            try:
                self.machine_number = settings.AUDIT_MACHINE_NUMBER
            # pylint: disable=broad-except
            except Exception:
            # pylint: enable=broad-except
                self.machine_number = 0
            sleep(2 * (self.machine_number + self.thread_id))
            if self.machine_number is not None and self.thread_id is not None:
                zombies = AuditProcessor.objects.filter(
                    seed_status=1,
                    completed__isnull=True,
                    machine=self.machine_number,
                    thread=self.thread_id
                )
                if zombies.count() > 0:
                    zombies.update(
                        seed_status=0,
                        machine=None,
                        thread=None,
                    )
            try:
                self.audit = AuditProcessor.objects.filter(seed_status=0, completed__isnull=True, machine=None, thread=None).order_by("audit__pause","id")[0]
            # pylint: disable=broad-except
            except Exception as e:
            # pylint: enable=broad-except
                logger.exception(e)
                print("no audits to seed at present")
                return
            self.force_data_refresh = self.audit.params.get("force_data_refresh")
            self.audit.seed_status = 1
            self.audit.machine = self.machine_number
            self.audit.thread = self.thread_id
            self.audit.save(update_fields=["seed_status", "machine", "thread"])
            self.process_seed()

    # pylint: disable=too-many-statements
    def process_seed(self):
        seed_file = self.audit.params.get("seed_file")
        if seed_file:
            if self.audit.audit_type in [0, 1]:
                return self.process_seed_video_file(seed_file)
            elif self.audit.audit_type == 2:
                return self.process_seed_channel_file(seed_file)
        self.audit.params["error"] = "seed list is empty"
        self.audit.completed = timezone.now()
        self.audit.seed_status = 2
        self.audit.pause = 0
        self.audit.save(update_fields=["params", "completed", "pause", "seed_status"])
        raise Exception("seed list is empty for this audit. {}".format(self.audit.id))

    # pylint: disable=too-many-statements
    def process_seed_channel_file(self, seed_file):
        try:
            f = AuditFileS3Exporter.get_s3_export_csv(seed_file)
        # pylint: disable=broad-except
        except Exception:
            # pylint: enable=broad-except
            self.audit.params["error"] = "can not open seed file"
            self.audit.completed = timezone.now()
            self.seed_status = 2
            self.audit.pause = 0
            self.audit.save(update_fields=["params", "completed", "pause", "seed_status"])
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
            if v_id and counter < self.MAX_SOURCE_CHANNELS_CAP and not v_id in processed_ids:
                processed_ids.append(v_id)
                if len(vids) >= self.MAX_SOURCE_CHANNELS:
                    self.clone_audit()
                    vids = []
                channel = AuditChannel.get_or_create(v_id)
                if channel.processed_time and (
                        self.force_data_refresh or channel.processed_time < timezone.now() - timedelta(days=30)):
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
            self.seed_status = 2
            self.audit.pause = 0
            self.audit.save(update_fields=["params", "completed", "pause", "seed_status"])
            raise Exception("no valid YouTube Channel URL's in seed file {}".format(seed_file))
        audit = self.audit
        audit.seed_status = 2
        audit.save(update_fields=["seed_status"])
        return vids
    # pylint: enable=too-many-statements

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

    def process_seed_video_file(self, seed_file):
        try:
            f = AuditFileS3Exporter.get_s3_export_csv(seed_file)
        # pylint: disable=broad-except
        except Exception:
        # pylint: enable=broad-except
            self.audit.params["error"] = "can not open seed file"
            self.audit.completed = timezone.now()
            self.audit.seed_status = 2
            self.audit.pause = 0
            self.audit.save(update_fields=["params", "completed", "pause", "seed_status"])
            raise Exception("can not open seed file {}".format(seed_file))
        reader = csv.reader(f)
        vids = []
        counter = 0
        resume_val = AuditVideoProcessor.objects.filter(audit=self.audit).count()
        print("processing seed file starting at position {}".format(resume_val))
        skipper = 0
        if resume_val > 0:
            for _ in reader:
                if skipper >= resume_val:
                    break
                skipper += 1
        for row in reader:
            avp = AuditUtils.get_avp_from_url(row[0], self.audit)
            if avp:
                counter+=1
                if len(vids) >= self.MAX_SOURCE_VIDEOS:
                    self.clone_audit()
                    vids = []
                vids.append(avp)
        if counter == 0 and resume_val == 0:
            self.audit.params["error"] = "no valid YouTube Video URL's in seed file"
            self.audit.seed_status = 2
            self.audit.completed = timezone.now()
            self.audit.pause = 0
            self.audit.save(update_fields=["params", "completed", "pause", "seed_status"])
            raise Exception("no valid YouTube Video URL's in seed file {}".format(seed_file))
        audit = self.audit
        audit.seed_status = 2
        audit.save(update_fields=["seed_status"])

    def clone_audit(self):
        self.num_clones += 1
        if not self.original_audit_name:
            self.original_audit_name = self.audit.params.get("name")
            if not self.original_audit_name:
                self.original_audit_name = self.audit.name
        self.audit.seed_status = 2
        self.audit.save(update_fields=["seed_status"])
        self.audit = AuditUtils.clone_audit(self.audit, self.num_clones, name=self.original_audit_name, seed_status=1)
        self.audit.machine = self.machine_number
        self.audit.thread = self.thread_id
        self.audit.save(update_fields=["machine", "thread"])

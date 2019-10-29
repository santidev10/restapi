import logging
import csv

from django.core.management.base import BaseCommand
from pid import PidFile
from pid import PidFileError

from es_components.connections import init_es_connection
from es_components.managers.channel import ChannelManager
from es_components.models.channel import Channel
from es_components.constants import Sections
from brand_safety.models import BadWordCategory
from audit_tool.models import BlacklistItem
from django.core.exceptions import  ValidationError


logger = logging.getLogger(__name__)


class Command(BaseCommand):

    def add_arguments(self, parser):
        parser.add_argument(
            "--filename",
            help="Name of TaskUs .csv file to import data from."
        )

    def handle(self, *args, **kwargs):
        try:
            file_name = kwargs["filename"]
        except KeyError:
            raise ValidationError("Argument 'filename' is required.")

        try:
            with PidFile("import_taskus_data.pid", piddir=".") as pid:
                channel_manager = ChannelManager(sections=(Sections.TASK_US_DATA,),
                                             upsert_sections=(Sections.TASK_US_DATA,))
                with open(file_name, "r") as f:
                    reader = csv.reader(f)
                    next(reader)
                    for row in reader:
                        channel_id = row[0].split('/')[-2]
                        iab_category_1 = row[1]
                        iab_category_2 = row[2]
                        moderation = row[3]
                        if moderation.lower().strip() == "unsafe":
                            try:
                                flag = BlacklistItem.get_or_create(channel_id, BlacklistItem.CHANNEL_ITEM)
                                flag_category = BadWordCategory.from_string(row[4])
                                flag.blacklist_category = {flag_category.id: 100}
                                flag.save()
                            except Exception:
                                pass
                        monetized = True if row[6].lower().strip() == "monetized" else None
                        scalable = row[7].capitalize().strip()
                        language = row[8].capitalize().strip() if row[8] != "Unknown" else ""
        except PidFileError:
            raise PidFileError

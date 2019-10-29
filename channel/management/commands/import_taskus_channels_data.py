import logging
import csv

from django.core.management.base import BaseCommand
from pid import PidFile
from pid import PidFileError

from es_components.connections import init_es_connection
from es_components.managers.channel import ChannelManager
from es_components.constants import Sections
from brand_safety.models import BadWordCategory
from audit_tool.models import BlacklistItem
from django.core.exceptions import  ValidationError
from utils.transform import populate_channel_task_us_data


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
                init_es_connection()
                channel_manager = ChannelManager(sections=(Sections.TASK_US_DATA,),
                                                 upsert_sections=(Sections.TASK_US_DATA,))
                all_channel_ids = set()
                channels_taskus_data_dict = {}
                with open(file_name, "r") as f:
                    reader = csv.reader(f)
                    next(reader)
                    for row in reader:
                        try:
                            channel_id = row[0].split('/')[-2]
                            all_channel_ids.add(channel_id)
                            current_channel_data = dict()
                            iab_category_1 = row[1].strip()
                            iab_category_2 = row[2].strip()
                            current_channel_data['categories'] = [iab_category_1]
                            if iab_category_2:
                                current_channel_data['categories'].append(iab_category_2)
                            moderation = row[3].lower().strip()
                            if moderation != "gray area":
                                current_channel_data['safe'] = True if moderation == "safe" else False
                            if moderation == "unsafe":
                                try:
                                    flag = BlacklistItem.get_or_create(channel_id, BlacklistItem.CHANNEL_ITEM)
                                    flag_category = BadWordCategory.from_string(row[4])
                                    flag.blacklist_category = {flag_category.id: 100}
                                    flag.save()
                                except Exception:
                                    pass
                            content_type = row[5].upper().strip()
                            current_channel_data['content_type'] = content_type
                            monetized = True if row[6].lower().strip() == "monetized" else None
                            if monetized:
                                current_channel_data['monetized'] = monetized
                            scalable = row[7].capitalize().strip()
                            current_channel_data['scalability'] = scalable
                            language = row[8].capitalize().strip() if row[8] != "Unknown" else ""
                            current_channel_data['language'] = language
                            channels_taskus_data_dict[channel_id] = current_channel_data
                        except Exception as e:
                            continue
                all_channels = channel_manager.get_or_create(list(all_channel_ids))
                for channel in all_channels:
                    populate_channel_task_us_data(channel, channels_taskus_data_dict[channel.main.id])
        except PidFileError:
            raise PidFileError

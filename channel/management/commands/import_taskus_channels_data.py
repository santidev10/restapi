import logging
import csv
import os

from django.core.management.base import BaseCommand
from pid import PidFile
from pid import PidFileError

from es_components.connections import init_es_connection
from es_components.managers.channel import ChannelManager
from es_components.managers.video import VideoManager
from es_components.constants import Sections
from brand_safety.models import BadWordCategory
from audit_tool.models import BlacklistItem
from django.core.exceptions import ValidationError
from django.conf import settings


logger = logging.getLogger(__name__)


class Command(BaseCommand):

    def add_arguments(self, parser):
        parser.add_argument(
            "--filename",
            help="Name of TaskUs .csv file to import data from."
        )

    def handle(self, *args, **kwargs):
        row_file_name = "taskus_row_number.txt"
        try:
            with open(row_file_name, "r") as f:
                row_number = int(f.readline())
        except Exception:
            row_number = 0
        try:
            file_name = kwargs["filename"]
        except KeyError:
            raise ValidationError("Argument 'filename' is required.")
        try:
            with PidFile("import_taskus_data.pid", piddir=".") as pid:
                init_es_connection()
                channel_manager = ChannelManager(sections=(Sections.TASK_US_DATA, Sections.GENERAL_DATA),
                                                 upsert_sections=(Sections.TASK_US_DATA, Sections.GENERAL_DATA))
                video_manager = VideoManager(sections=(Sections.GENERAL_DATA),
                                             upsert_sections=(Sections.GENERAL_DATA))
                all_channel_ids = set()
                channels_taskus_data_dict = {}
                channels_iab_categories_dict = {}
                row_counter = row_number
                channel_counter = 0
                vid_counter = 0
                rows_parsed = 0
                with open(os.path.join(settings.BASE_DIR, file_name), "r") as f:
                    reader = csv.reader(f)
                    next(reader)
                    while row_number > 0:
                        next(reader)
                        row_number -= 1
                    for row in reader:
                        channel_id = row[0].split('/')[-2]
                        print(f"Row {rows_parsed}: {channel_id}")
                        all_channel_ids.add(channel_id)
                        current_channel_taskus_data = dict()
                        iab_category_1 = row[1].strip()
                        try:
                            iab_category_2 = row[2].strip()
                        except Exception:
                            iab_category_2 = None
                        current_channel_iab_categories = [iab_category_1]
                        if iab_category_2:
                            current_channel_iab_categories.append(iab_category_2)
                        try:
                            moderation = row[3].lower().strip()
                            if moderation == "safe":
                                current_channel_taskus_data['is_safe'] = True
                            elif moderation == "unsafe":
                                current_channel_taskus_data['is_safe'] = False
                                flag = BlacklistItem.get_or_create(channel_id, BlacklistItem.CHANNEL_ITEM)
                                flag_category = BadWordCategory.from_string(row[4].lower().strip())
                                flag.blacklist_category = {flag_category.id: 100}
                                flag.save()
                        except Exception:
                            pass
                        try:
                            content_type = row[5].upper().strip()
                            is_user_generated_content = True if content_type == "UGC" else False
                            current_channel_taskus_data['is_user_generated_content'] = is_user_generated_content
                        except Exception:
                            pass
                        try:
                            monetized = True if row[6].lower().strip() == "monetized" else None
                            if monetized:
                                current_channel_taskus_data['monetized'] = monetized
                        except Exception:
                            pass
                        try:
                            scalable = row[7].capitalize().strip()
                            if scalable:
                                current_channel_taskus_data['scalable'] = True if scalable == "Scalable" else False
                        except Exception:
                            pass
                        try:
                            language = row[8].capitalize().strip() if row[8] != "Unknown" else ""
                            if language:
                                current_channel_taskus_data['language'] = language
                        except Exception:
                            pass
                        channels_taskus_data_dict[channel_id] = current_channel_taskus_data
                        channels_iab_categories_dict[channel_id] = current_channel_iab_categories
                        if len(all_channel_ids) >= 1000:
                            all_channels = channel_manager.get(list(all_channel_ids))
                            all_channels = list(filter(None, all_channels))
                            for channel in all_channels:
                                channel_counter += 1
                                channel.populate_task_us_data(**channels_taskus_data_dict[channel.main.id])
                                channel.populate_general_data(
                                    iab_categories=channels_iab_categories_dict[channel.main.id])
                                videos_filter = video_manager.by_channel_ids_query(channel.main.id)
                                channel_videos = video_manager.search(filters=videos_filter).scan()
                                upsert_videos = []
                                for video in channel_videos:
                                    video.populate_general_data(
                                        iab_categories=channels_iab_categories_dict[channel.main.id])
                                    upsert_videos.append(video)
                                    vid_counter += 1
                                video_manager.upsert(upsert_videos)
                                channel_manager.upsert([channel])
                                row_counter += 1
                                with open(row_file_name, "w+") as row_file:
                                    row_file.write(str(row_counter))
                                print(f"Upserted videos and channel for {channel.main.id}")
                                print(f"Number of channels with videos parsed: {channel_counter}")
                                print(f"row_counter: {row_counter}")
                            print(f"Number of channels upserted: {channel_counter}")
                            print(f"Number of videos upserted: {vid_counter}")
                            all_channel_ids = set()
                            channels_taskus_data_dict = {}
                            channels_iab_categories_dict = {}
                        rows_parsed += 1
                        print(f"Number of rows parsed: {rows_parsed}")
        except PidFileError:
            raise PidFileError

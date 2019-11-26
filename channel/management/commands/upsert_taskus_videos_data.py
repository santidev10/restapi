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
        row_file_name = "taskus_videos_parsing_row_number.txt"
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
            with PidFile("upsert_taskus_videos_data.pid", piddir=".") as pid:
                init_es_connection()
                channel_manager = ChannelManager(sections=(Sections.TASK_US_DATA, Sections.GENERAL_DATA,
                                                           Sections.MONETIZATION, Sections.CUSTOM_PROPERTIES),
                                                 upsert_sections=(Sections.TASK_US_DATA, Sections.GENERAL_DATA,
                                                                  Sections.MONETIZATION, Sections.CUSTOM_PROPERTIES))
                video_manager = VideoManager(sections=(Sections.GENERAL_DATA,),
                                             upsert_sections=(Sections.GENERAL_DATA,))
                row_counter = row_number
                vid_counter = 0
                rows_parsed = 0
                channels_row_dict = {}
                with open(os.path.join(settings.BASE_DIR, file_name), "r") as f:
                    reader = csv.reader(f)
                    next(reader)
                    while row_number > 0:
                        next(reader)
                        row_number -= 1
                    for row in reader:
                        channel_id = row[0].split('/')[-2]
                        iab_category_1 = row[1].strip().replace(" and ", " & ").title()
                        try:
                            iab_category_2 = row[2].strip().replace(" and ", " & ").title()
                        except Exception:
                            iab_category_2 = None
                        current_channel_iab_categories = [iab_category_1]
                        if iab_category_2:
                            current_channel_iab_categories.append(iab_category_2)
                        print(f"Parsing videos for row {row_counter}: {channel_id}")
                        try:
                            videos_filter = video_manager.by_channel_ids_query(channel_id)
                            channel_videos = video_manager.search(filters=videos_filter).scan()
                            upsert_videos = []
                            for video in channel_videos:
                                video.populate_general_data(
                                    iab_categories=current_channel_iab_categories)
                                upsert_videos.append(video)
                                vid_counter += 1
                            video_manager.upsert(upsert_videos)
                            print(f"Upserted {len(upsert_videos)} videos for Channel {channel_id}.")
                            print(f"Finished upserting videos up to Row #{row_counter}.")
                            print(f"Total videos upserted: {vid_counter}.")
                        except Exception as e:
                            pass
                        row_counter += 1
                        with open(row_file_name, "w+") as row_file:
                            row_file.write(str(row_counter))
        except PidFileError:
            raise PidFileError

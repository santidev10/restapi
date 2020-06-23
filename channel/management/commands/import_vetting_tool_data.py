import copy
import csv
import os

from django.conf import settings
from django.core.exceptions import ValidationError
from django.core.management.base import BaseCommand

from audit_tool.models import AuditAgeGroup
from audit_tool.models import AuditContentType
from audit_tool.models import AuditGender
from brand_safety.languages import LANGUAGES
from es_components.connections import init_es_connection
from es_components.constants import Sections
from es_components.iab_categories import IAB_TIER3_CATEGORIES_MAPPING
from es_components.managers.channel import ChannelManager


# pylint: disable=too-many-instance-attributes
class Command(BaseCommand):
    def __init__(self, stdout=None, stderr=None, no_color=False, force_color=False):
        super().__init__(stdout, stderr, no_color, force_color)
        self.cursor_filename = "channel_import_cursor.txt"
        self.bad_rows_filename = "channel_import_bad_rows.csv"
        self.channel_ids = []
        self.invalid_categories = []
        self.iab_categories = []
        self.parsed_count = 0
        self.upsert_count = 0
        self.missed_upserts_count = 0
        self.chunk_size = 1000
        self.bad_rows_count = 0

        # all maps channel_id to dicts
        self.main_data_map = {}
        self.general_data_map = {}
        self.task_us_data_map = {}

        self.reader = None
        self.row_cursor = None
        self.filename = None
        self.channel_manager = None

    def add_arguments(self, parser):
        parser.add_argument(
            "--filename",
            help="Name of the Vetting Tool .csv file to import data from"
        )

    # pylint: disable=too-many-statements
    def handle(self, *args, **kwargs):
        # initialize stuff
        self.init_filename(*args, **kwargs)
        self.init_cursor()
        self.init_iab_categories_list()
        self.init_channel_manager()

        with open(os.path.join(settings.BASE_DIR, self.filename), "r") as file:
            self.reader = csv.reader(file)
            self.verify_structure()
            self.fast_forward_to_cursor()

            for row in self.reader:
                self.parsed_count += 1
                self.row_cursor += 1

                general_data = {}
                task_us_data = {}

                # channel id
                channel_id = None
                channel_id = self.get_channel_id(row)
                if not channel_id:
                    reason = "Invalid channel id: {}".format(channel_id)
                    self.write_bad_row(row, reason=reason)
                    self.bad_rows_count += 1
                    continue
                self.channel_ids.append(channel_id)

                # language
                channel_lang_code = None
                channel_language = None
                channel_lang_code = self.get_channel_lang_code(row)
                if channel_lang_code:
                    task_us_data["lang_code"] = channel_lang_code
                    general_data["lang_codes"] = [channel_lang_code]
                    general_data["top_lang_code"] = channel_lang_code
                    # set language field
                    channel_language = self.get_channel_language(channel_lang_code)
                    general_data["top_language"] = channel_language

                # category
                channel_category = None
                channel_category = self.get_channel_category(row)
                if channel_category:
                    general_data["iab_categories"] = [channel_category]
                    task_us_data["iab_categories"] = [channel_category]
                else:
                    raw_category = self.get_raw_channel_category(row)
                    if raw_category:
                        reason = "Invalid category:{}".format(raw_category)
                        self.write_bad_row(row, reason=reason)
                        self.bad_rows_count += 1
                        continue

                # age group
                age_group_id = None
                age_group_id = self.get_channel_age_group_id(row)
                if age_group_id is not None:
                    task_us_data["age_group"] = age_group_id

                # gender
                channel_gender_id = None
                channel_gender_id = self.get_channel_gender_id(row)
                if channel_gender_id is not None:
                    task_us_data["gender"] = channel_gender_id

                # content type
                content_type_id = None
                content_type_id = self.get_content_type_id(row)
                if content_type_id is not None:
                    task_us_data["content_type"] = content_type_id
                    if content_type_id in [1, 2]:
                        task_us_data["is_user_generated_content"] = True

                self.general_data_map[channel_id] = general_data
                self.task_us_data_map[channel_id] = task_us_data

                # chunk upsert
                if len(self.channel_ids) >= self.chunk_size:
                    self.upsert_channels()
                    self.write_row_cursor_position()

            # upsert remaining channels
            self.upsert_channels()
            self.write_row_cursor_position()
            print("rows parsed:", self.parsed_count)
            print("bad_rows:", self.bad_rows_count)
            print("upserts:", self.upsert_count)
            print("missed upserts:", self.missed_upserts_count)
            print("invalid categories:", set(self.invalid_categories))

    # pylint: enable=too-many-statements

    def upsert_channels(self):
        if not self.channel_ids:
            print("no channels to update!")
            return

        print("Attempting upsert of {} channel records from ES...".format(len(self.channel_ids)))

        channels = self.channel_manager.get(self.channel_ids, skip_none=True)
        upsert_count = 0
        for channel in channels:
            if not channel.main.id:
                print("channel missing main.id: {}".format(channel))
                continue
            upsert_count += 1
            channel_id = channel.main.id

            # handle general data
            general_data = self.general_data_map.get(channel_id, None)
            if general_data:
                # append to existing lang codes
                if channel.general_data.lang_codes:
                    lang_codes = general_data.setdefault("lang_codes", [])
                    lang_codes = lang_codes + list(channel.general_data.lang_codes)
                    lang_codes = list(set(lang_codes))
                    general_data["lang_codes"] = lang_codes
                channel.populate_general_data(**general_data)
            else:
                print(f"channel data missing in general_data map. id: {channel_id}")

            # handle task us data
            task_us_data = self.task_us_data_map.get(channel_id, None)
            if task_us_data:
                channel.populate_task_us_data(**task_us_data)
            else:
                print(f"channel data missing in task_us_data map. id: {channel_id}")

        self.channel_manager.upsert(channels)

        # update counts
        self.upsert_count += upsert_count
        missed_upserts_count = len(self.channel_ids) - upsert_count
        self.missed_upserts_count += missed_upserts_count
        print("Upserted {} channels".format(self.upsert_count))
        print("Missed {} upserts:".format(missed_upserts_count))

        self.channel_ids = []
        self.general_data_map = {}
        self.task_us_data_map = {}

    def get_channel_age_group_id(self, row: list) -> str:
        raw_age_group = row[4]
        if not raw_age_group:
            return None
        try:
            channel_age_group_id = AuditAgeGroup.to_id[raw_age_group.lower()]
        except KeyError:
            return None
        return channel_age_group_id

    def get_channel_gender_id(self, row: list) -> str:
        raw_gender = row[5]
        if not raw_gender:
            return None
        try:
            channel_gender_id = AuditGender.to_id[raw_gender.lower()]
        except KeyError:
            return None
        return channel_gender_id

    def get_content_type_id(self, row: list) -> str:
        content_type = row[6]
        if not content_type:
            return None
        content_type = "Regular UGC" \
            if content_type.lower() == "ugc" \
            else content_type
        try:
            content_type_id = AuditContentType.to_id[content_type.lower()]
        except KeyError:
            return None
        return content_type_id

    def get_raw_channel_category(self, row: list) -> str:
        return row[3]

    def get_channel_category(self, row: list) -> str:
        category = self.get_raw_channel_category(row)
        if not category:
            return None
        if self.is_valid_category(category):
            return category
        self.invalid_categories.append(category)
        return None

    def is_valid_category(self, category):
        return category in self.iab_categories

    def get_channel_lang_code(self, row: list) -> str:
        raw_lang_code = row[2]
        lowered_lang_code = raw_lang_code.lower()
        if lowered_lang_code in LANGUAGES.keys():
            return lowered_lang_code
        return None

    def get_channel_language(self, code: str) -> str:
        return LANGUAGES[code.lower()]

    def get_channel_id(self, row: list) -> str:
        return row[0].split("/")[-1]

    def get_channel_title(self, row: list) -> str:
        return row[1]

    def init_filename(self, *args, **kwargs):
        print("initializing filename...")
        try:
            self.filename = kwargs["filename"]
        except KeyError:
            raise ValidationError("Argument 'filename' is required.")
        print("filename:", self.filename)

    def init_cursor(self):
        print("initializing cursor...")
        try:
            with open(os.path.join(settings.BASE_DIR, self.cursor_filename), "r") as file:
                self.row_cursor = int(file.readline())
        # pylint: disable=broad-except
        except Exception:
            # pylint: enable=broad-except
            self.row_cursor = 0
        print("cursor at:", self.row_cursor)

    def init_iab_categories_list(self, categories=None):
        if categories is None:
            print("initializing IAB categories list...")
            categories = copy.deepcopy(IAB_TIER3_CATEGORIES_MAPPING)
        for key, value in categories.items():
            if isinstance(key, str):
                self.iab_categories.append(key)
            if isinstance(value, str):
                self.iab_categories.append(value)
            if isinstance(value, list) and value:
                self.iab_categories += value
            if isinstance(value, dict):
                self.init_iab_categories_list(value)

    def fast_forward_to_cursor(self):
        print("fast-forwarding to cursor...")
        while self.row_cursor > 0:
            next(self.reader)
            self.row_cursor -= 1
        print("cursor at:", self.row_cursor)

    def verify_structure(self):
        print("verifying csv structure...")
        columns = [
            "URL",
            "Title",
            "Language",
            "Category",
            "Age_Group",
            "Gender",
            "Content_Type"
        ]
        header_row = next(self.reader)
        trimmed = list(filter(None, header_row))
        if trimmed != columns:
            msg = "Bad structure. Column order should be: {}. Detected order: {}" \
                .format(", ".join(columns), ", ".join(trimmed))
            raise ValidationError(msg)

    def init_channel_manager(self):
        init_es_connection()
        self.channel_manager = ChannelManager(
            sections=(
                Sections.TASK_US_DATA,
                Sections.GENERAL_DATA,
            ),
            upsert_sections=(
                Sections.TASK_US_DATA,
                Sections.GENERAL_DATA,
            )
        )

    def write_row_cursor_position(self):
        with open(os.path.join(settings.BASE_DIR, self.cursor_filename), "w+") as cursor_file:
            cursor_file.write(str(self.row_cursor))

    def write_bad_row(self, row: list, reason=""):
        with open(os.path.join(settings.BASE_DIR, self.bad_rows_filename), "a+") as file:
            writer = csv.writer(file)
            writer.writerow(row + [self.row_cursor, reason])
# pylint: enable=too-many-instance-attributes

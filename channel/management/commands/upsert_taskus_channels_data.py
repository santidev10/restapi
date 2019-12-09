import logging
import csv
import os

from django.core.management.base import BaseCommand
from pid import PidFile
from pid import PidFileError

from es_components.connections import init_es_connection
from es_components.managers.channel import ChannelManager
from es_components.constants import Sections
from brand_safety.models import BadWordCategory
from audit_tool.models import BlacklistItem
from django.core.exceptions import ValidationError
from django.conf import settings

from es_components.iab_categories import IAB_TIER3_CATEGORIES_MAPPING

from audit_tool.models import AuditChannel


logger = logging.getLogger(__name__)


class Command(BaseCommand):

    def add_arguments(self, parser):
        parser.add_argument(
            "--filename",
            help="Name of TaskUs .csv file to import data from."
        )

    def handle(self, *args, **kwargs):
        row_file_name = "taskus_channels_parsing_row_number.txt"
        bad_rows_file_name = "bad_taskus_rows.csv"
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
            with PidFile("upsert_taskus_channels_data.pid", piddir=".") as pid:
                init_es_connection()
                channel_manager = ChannelManager(sections=(Sections.TASK_US_DATA, Sections.GENERAL_DATA,
                                                           Sections.MONETIZATION, Sections.CUSTOM_PROPERTIES),
                                                 upsert_sections=(Sections.TASK_US_DATA, Sections.GENERAL_DATA,
                                                                  Sections.MONETIZATION, Sections.CUSTOM_PROPERTIES))
                all_channel_ids = []
                channels_taskus_data_dict = {}
                channels_iab_categories_dict = {}
                channels_monetization_dict = {}
                row_counter = row_number
                channel_counter = 0
                rows_parsed = 0
                with open(os.path.join(settings.BASE_DIR, file_name), "r") as f:
                        reader = csv.reader(f)
                        while row_number > 0:
                            next(reader)
                            row_number -= 1
                        for row in reader:
                            valid_row = True
                            invalid_reasons = []
                            channel_id = row[0].split('/')[-2]
                            row_counter += 1
                            print(f"Row {row_counter}: {channel_id}")
                            all_channel_ids.append(channel_id)
                            current_channel_taskus_data = dict()
                            try:
                                iab_category_1 = row[1].strip().replace(" and ", " & ")
                                iab_category_1 = iab_category_1 \
                                    if iab_category_1 in IAB_TIER3_CATEGORIES_MAPPING \
                                    else ""
                            except Exception:
                                iab_category_1 = None
                            if not iab_category_1:
                                valid_row = False
                                invalid_reasons.append("No valid IAB Tier 1 Category found.")

                            try:
                                if iab_category_1:
                                    iab_category_2 = row[2].strip().replace(" and ", " & ")
                                    iab_category_2 = iab_category_2 \
                                        if iab_category_2 in IAB_TIER3_CATEGORIES_MAPPING[iab_category_1] \
                                        else ""
                            except Exception:
                                iab_category_2 = None
                            if row[2] and not iab_category_2:
                                valid_row = False
                                invalid_reasons.append("IAB Tier 2 Category doesn't follow IAB standards.")

                            try:
                                if iab_category_2:
                                    iab_category_3 = row[3].strip().replace(" and ", " & ")
                                    iab_category_3 = iab_category_3 \
                                        if iab_category_3 in IAB_TIER3_CATEGORIES_MAPPING[iab_category_1][iab_category_2] \
                                        else ""
                            except Exception:
                                iab_category_3 = None
                            if row[3] and not iab_category_3:
                                valid_row = False
                                invalid_reasons.append("IAB Tier 3 Category doesn't follow IAB standards.")

                            current_channel_iab_categories = []
                            if iab_category_1:
                                current_channel_iab_categories.append(iab_category_1)
                            if iab_category_2:
                                current_channel_iab_categories.append(iab_category_2)
                            if iab_category_3:
                                current_channel_iab_categories.append(iab_category_3)
                            current_channel_taskus_data['iab_categories'] = current_channel_iab_categories

                            try:
                                moderation = row[4].lower().strip()
                                if moderation == "safe":
                                    current_channel_taskus_data['is_safe'] = True
                                elif moderation == "unsafe":
                                    flag_category = BadWordCategory.objects.get(name=row[5].lower().strip())
                                    current_channel_taskus_data['is_safe'] = False
                                    flag = BlacklistItem.get_or_create(channel_id, BlacklistItem.CHANNEL_ITEM)
                                    if flag.blacklist_category:
                                        flag.blacklist_category[flag_category.id] = 100
                                    else:
                                        flag.blacklist_category = {flag_category.id: 100}
                                    flag.save()
                            except Exception:
                                invalid_reasons.append("Moderation reason is invalid.")

                            try:
                                content_type = row[6].upper().strip()
                                is_user_generated_content = True if content_type == "UGC" else False
                                current_channel_taskus_data['is_user_generated_content'] = is_user_generated_content
                            except Exception:
                                pass

                            try:
                                is_monetizable = True if row[7].lower().strip() == "monetized" else None
                                if is_monetizable:
                                    channels_monetization_dict[channel_id] = is_monetizable
                            except Exception:
                                pass

                            try:
                                scalable = row[8].capitalize().strip()
                                if scalable:
                                    current_channel_taskus_data['scalable'] = True if scalable == "Scalable" else False
                            except Exception:
                                pass

                            try:
                                language = row[8].capitalize().strip() if row[9] != "Unknown" else ""
                                if language:
                                    current_channel_taskus_data['language'] = language
                            except Exception:
                                pass

                            if not valid_row:
                                with open(os.path.join(settings.BASE_DIR, bad_rows_file_name), "a+") as bad_rows_file:
                                    bad_rows_writer = csv.writer(bad_rows_file)
                                    bad_rows_writer.writerow(row + [row_counter+1] + invalid_reasons)

                            channels_taskus_data_dict[channel_id] = current_channel_taskus_data
                            channels_iab_categories_dict[channel_id] = current_channel_iab_categories
                            rows_parsed += 1
                            print(f"Number of rows parsed: {rows_parsed}")
                            try:
                                if len(all_channel_ids) >= 10000:
                                    all_channels = channel_manager.get_or_create(all_channel_ids)
                                    for channel in all_channels:
                                        chan_id = channel.main.id
                                        channel_counter += 1
                                        channel.populate_task_us_data(**channels_taskus_data_dict[chan_id])
                                        channel.populate_general_data(
                                            iab_categories=channels_iab_categories_dict[chan_id]
                                        )
                                        channel.populate_custom_properties(is_tracked=True)
                                        if chan_id in channels_monetization_dict:
                                            channel.populate_monetization(
                                                is_monetizable=channels_monetization_dict[chan_id]
                                            )
                                            try:
                                                audit_channel = AuditChannel.get_or_create(chan_id, create=False).auditchannelmeta
                                                audit_channel.monetised = True
                                                audit_channel.save()
                                            except Exception as e:
                                                pass
                                        print(f"Updated fields for {chan_id}")
                                        print(f"Parsed {channel_counter} channels.")
                                    channel_manager.upsert(all_channels)
                                    with open(row_file_name, "w+") as row_file:
                                        row_file.write(str(row_counter))

                                    print(f"Upserted {channel_counter} channels.")
                                    print(f"Upserted channels up to Row #{row_counter}")
                                    all_channel_ids = []
                                    channels_taskus_data_dict = {}
                                    channels_iab_categories_dict = {}
                                    channels_monetization_dict = {}
                                    return
                            except Exception as e:
                                raise e
        except PidFileError:
            raise PidFileError

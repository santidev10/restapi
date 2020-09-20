import csv
import logging
import os

from django.conf import settings
from django.core.exceptions import ValidationError
from django.core.management.base import BaseCommand
from pid import PidFile
from pid import PidFileError

from audit_tool.models import AuditAgeGroup
from audit_tool.models import AuditChannel
from audit_tool.models import AuditContentType
from audit_tool.models import AuditContentQuality
from audit_tool.models import AuditGender
from audit_tool.models import BlacklistItem
from brand_safety.languages import LANGUAGES
from brand_safety.models import BadWordCategory
from django.utils import timezone
from es_components.connections import init_es_connection
from es_components.constants import Sections
from es_components.iab_categories import IAB_TIER2_SET
from es_components.managers.channel import ChannelManager

logger = logging.getLogger(__name__)


class Command(BaseCommand):

    def add_arguments(self, parser):
        parser.add_argument(
            "--filename",
            help="Name of TaskUs .csv file to import data from."
        )

    def handle(self, *args, **kwargs):
        channel_ids = []
        channels_data = {}
        file_name = "ViewIQ upload 091720.csv"
        with open(file_name, "r") as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                try:
                    cid = row[0].split("/")[-1]
                    if len(cid) != 24 or cid[:2] != "UC":
                        continue
                    title = row[1]
                    categories = self.get_categories(row[2])
                    language = self.get_language(row[3].lower())
                    age_group = self.get_age_group(row[4].lower())
                    gender = self.get_gender(row[5].lower())
                    content_type = self.get_content_type(row[5].lower())
                    content_quality = self.get_content_quality(row[5].lower())
                    channel_ids.append(cid)
                    channels_data[cid] = {
                        "title": title,
                        "categories": categories,
                        "language": language,
                        "age_group": age_group,
                        "gender": gender,
                        "content_type": content_type,
                        "content_quality": content_quality,
                        "last_vetted_at": timezone.now()
                    }
                # pylint: disable=broad-except
                except Exception:
                # pylint: enable=broad-except
                    continue

    @staticmethod
    def get_categories(categories_string):
        categories = [category for category in categories_string.split(", ") if category in IAB_TIER2_SET]
        return categories

    @staticmethod
    def get_language(lang):
        return lang.lower() if lang.lower() in LANGUAGES else None

    @staticmethod
    def get_age_group(age_group_string):
        try:
            age_group_id = AuditAgeGroup.to_id[age_group_string.lower()]
            return age_group_id
        # pylint: disable=broad-except
        except Exception:
        # pylint: enable=broad-except
            return None

    @staticmethod
    def get_gender(gender):
        try:
            gender_id = AuditGender.to_id[gender.lower()]
            return gender_id
        # pylint: disable=broad-except
        except Exception:
        # pylint: enable=broad-except
            return None

    @staticmethod
    def get_content_type(content_type):
        try:
            content_type_id = AuditContentType.to_id[content_type.lower()]
            return content_type_id
        # pylint: disable=broad-except
        except Exception:
        # pylint: enable=broad-except
            return None

    @staticmethod
    def get_content_quality(content_quality):
        try:
            content_quality_id = AuditContentQuality.to_id[content_quality.lower()]
            return content_quality_id
        # pylint: disable=broad-except
        except Exception:
        # pylint: enable=broad-except
            return None

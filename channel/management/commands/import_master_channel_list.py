import csv
import logging

from django.core.exceptions import ValidationError
from django.core.management.base import BaseCommand

from audit_tool.models import AuditAgeGroup
from audit_tool.models import AuditContentType
from audit_tool.models import AuditContentQuality
from audit_tool.models import AuditGender
from brand_safety.languages import LANGUAGES
from brand_safety.models import BadWordCategory
from django.utils import timezone
from es_components.constants import Sections
from es_components.iab_categories import IAB_TIER2_SET
from es_components.managers.channel import ChannelManager

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    all_brand_safety_category_ids = BadWordCategory.objects.values_list("id", flat=True)
    REVIEW_SCORE_THRESHOLD = 79
    BATCH_SIZE = 10

    def add_arguments(self, parser):
        parser.add_argument(
            "--filename",
            help="Name of Channel List .csv file to import data from."
        )

    def handle(self, *args, **kwargs):
        channel_ids = []
        channels_data = {}
        try:
            file_name = kwargs["filename"]
        except KeyError:
            raise ValidationError("Argument 'filename' is required.")
        with open(file_name, "r") as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                try:
                    cid = row[0].split("/")[-1]
                    if len(cid) != 24 or cid[:2] != "UC":
                        continue
                    title = row[1]
                    categories = self.get_categories(row[2].strip())
                    lang_code = self.get_language(row[3].strip().lower())
                    age_group = self.get_age_group(row[4].strip().lower())
                    gender = self.get_gender(row[5].strip().lower())
                    content_type = self.get_content_type(row[6].strip().lower())
                    content_quality = self.get_content_quality(row[7].strip().lower())
                    channel_ids.append(cid)
                    channels_data[cid] = {
                        "general_data": {
                            "title": title
                        },
                        "task_us_data": {
                            "is_safe": True,
                            "iab_categories": categories,
                            "lang_code": lang_code,
                            "age_group": age_group,
                            "gender": gender,
                            "content_type": content_type,
                            "content_quality": content_quality,
                            "last_vetted_at": timezone.now()
                        }
                    }
                # pylint: disable=broad-except
                except Exception:
                # pylint: enable=broad-except
                    continue
        manager = ChannelManager(sections=(Sections.TASK_US_DATA, Sections.GENERAL_DATA, Sections.BRAND_SAFETY),
                                 upsert_sections=(Sections.TASK_US_DATA, Sections.GENERAL_DATA, Sections.BRAND_SAFETY))
        logger.info(f"Fetching {self.BATCH_SIZE} channels.")
        channels = manager.get_or_create(channel_ids[:self.BATCH_SIZE])
        for channel in channels:
            channel_id = channel.main.id
            logger.info(f"Populating data for Channel: {channel_id}")
            channel_data = channels_data[channel_id]
            channel.populate_general_data(**channel_data["general_data"])
            channel.populate_task_us_data(**channel_data["task_us_data"])
            try:
                bs_data = channel.brand_safety
                item_overall_score = bs_data.overall_score
                pre_limbo_score = bs_data.pre_limbo_score
                previous_blacklist_categories = channel.task_us_data.brand_safety or []
            except (IndexError, AttributeError):
                previous_blacklist_categories = []
                item_overall_score = None
                pre_limbo_score = None
            new_blacklist_categories, should_rescore = self.save_brand_safety(previous_blacklist_categories)
            brand_safety_category_overall_scores = self._get_brand_safety(new_blacklist_categories)
            channel.task_us_data["brand_safety"] = [None]
            brand_safety_limbo = self._get_brand_safety_limbo(item_overall_score, pre_limbo_score)
            channel.populate_brand_safety(
                rescore=should_rescore,
                categories=brand_safety_category_overall_scores,
                **brand_safety_limbo
            )
        logger.info(f"Upserting {self.BATCH_SIZE} channels.")
        manager.upsert(channels)
        logger.info(f"Finished upserting channels.")

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

    @staticmethod
    def save_brand_safety(previous_brand_safety: list) -> tuple:
        """
        Will rescore the document if there is a change in brand safety
        :param previous_brand_safety: list of brand safety categories before current vetting
        :return: list -> Brand safety category ids
        """
        should_rescore = False
        new_vetted_brand_safety = set()
        # Rescore if any blacklist categories changed
        if new_vetted_brand_safety != set([str(s) for s in previous_brand_safety]):
            should_rescore = True
        return list(new_vetted_brand_safety), should_rescore

    def _get_brand_safety_limbo(self, overall_score, pre_limbo_score):
        """
        Determine if the vetting item should be reviewed.

        :param brand_safety_data: BrandSafety document section data
        :return:
        """
        limbo_data = {}
        # brand safety may be saved as [None]
        safe = True

        # If vetting agrees with pre_limbo_score, limbo_status is resolved
        if pre_limbo_score is not None:
            if (safe and pre_limbo_score > self.REVIEW_SCORE_THRESHOLD) \
                    or (not safe and pre_limbo_score < self.REVIEW_SCORE_THRESHOLD):
                limbo_data["limbo_status"] = False
        # System scored as not safe but vet marks as safe. Because of discrepancy, mark in limbo
        elif overall_score is not None:
            if overall_score <= self.REVIEW_SCORE_THRESHOLD and safe:
                limbo_data = {
                    "limbo_status": True,
                    "pre_limbo_score": overall_score,
                }
        return limbo_data

    def _get_brand_safety(self, blacklist_categories: list):
        """
        Get updated brand safety categories based on blacklist_categories
        If category is in blacklist_category, will have a score of 0. Else will have a score of 100
        :param blacklist_categories: list of blacklist categories from vetting
        :return:
        """
        # Brand safety categories that are not sent with vetting data are implicitly brand safe categories
        reset_brand_safety = set(self.all_brand_safety_category_ids) - set(
            [int(category) for category in blacklist_categories])
        brand_safety_category_overall_scores = {
            str(category_id): {
                "category_score": 100 if category_id in reset_brand_safety else 0
            }
            for category_id in self.all_brand_safety_category_ids
        }
        return brand_safety_category_overall_scores

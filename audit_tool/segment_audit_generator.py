import logging

from django.utils import timezone
from audit_tool.models import get_hash_name
from audit_tool.models import AuditCategory
from audit_tool.models import AuditChannelMeta
from audit_tool.models import AuditVideoMeta
from audit_tool.models import AuditProcessor
from audit_tool.utils.audit_utils import AuditUtils
from brand_safety.auditors.utils import AuditUtils as BrandSafetyUtils
from segment.models import CustomSegment
from utils.utils import chunks_generator
from utils.youtube_api import YoutubeAPIConnector
from utils.db.get_exists import get_exists

logger = logging.getLogger(__name__)


class SegmentAuditGenerator(object):
    BATCH_SIZE = 1000
    segment = None
    audit_processor_type = None
    audit_model = None
    audit_meta_model = None
    audit_vetting_model = None
    meta_model_instantiator = None
    id_field = None
    language_mapping = None
    country_mapping = None
    category_mapping = None
    youtube_connector = None
    # Raw sql
    select_fields = None
    table_name = None

    def __init__(self, segment_id, data_field="video"):
        # video_id, channel_id
        self.id_field = data_field + "_id"
        try:
            self.segment = CustomSegment.objects.get(id=segment_id)
            self.audit_model = self.segment.audit_utils.model
            self.audit_meta_model = self.segment.audit_utils.meta_model
            self.audit_vetting_model = self.segment.audit_utils.vetting_model
        except CustomSegment.DoesNotExist:
            raise ValueError(f"generate_audit_items called for segment: {segment_id} does not exist.")
        self.category_mapping = AuditUtils.get_audit_category_mapping()
        self.country_mapping = AuditUtils.get_audit_country_mapping()
        self.language_mapping = AuditUtils.get_audit_language_mapping()
        if data_field == "video":
            self.audit_processor_type = 1
            self.youtube_connector = YoutubeAPIConnector().obtain_videos
            self.meta_model_instantiator = self.instantiate_video_meta_model
            self.select_fields = "id,video_id"
            self.table_name = "audit_tool_auditvideo"
        elif data_field == "channel":
            self.audit_processor_type = 2
            self.youtube_connector = YoutubeAPIConnector().obtain_channels
            self.meta_model_instantiator = self.instantiate_channel_meta_model
            self.select_fields = "id,channel_id"
            self.table_name = "audit_tool_auditchannel"
        else:
            raise ValueError(f"Unsupported data field: {data_field}")

    def run(self, item_ids=None):
        now = timezone.now()
        defaults = {
            "name": self.segment.title,
            "audit_type": self.audit_processor_type,
            "source": 1,
        }
        # Set timestamps to avoid automatic processing of audit scripts
        defaults["started"] = defaults["updated"] = defaults["completed"] = now
        audit_processor, created = AuditProcessor.objects.get_or_create(id=self.segment.audit_id, defaults=defaults)
        if created:
            self.segment.audit_id = audit_processor.id
            self.segment.save()
        if item_ids is None:
            item_ids = self.segment.get_extract_export_ids()
        for batch in chunks_generator(item_ids, size=self.BATCH_SIZE):
            try:
                if self.audit_processor_type == 1:
                    batch = [_id for _id in batch if len(_id) <= 11]
                else:
                    batch = [_id for _id in batch if len(_id) == 24]
                # Get the ids of audit Channel / Video items we need to create
                # tuple (item.id, item.channel_id)
                rows = get_exists(item_ids=batch, model_name=self.table_name,
                                  select_fields=self.select_fields, where_id_field=self.id_field)
                try:
                    existing_audit_ids, existing_item_ids = list(zip(*rows))
                except ValueError:
                    # No rows returned
                    existing_audit_ids, existing_item_ids = [], []

                # Generate only new audit / audit meta items with video / channel id
                to_create_ids = set(batch) - set(existing_item_ids)
                data = self.retrieve_youtube(self.youtube_connector, to_create_ids)

                audit_model_to_create = [self.instantiate_audit_model(item, self.id_field, self.audit_model) for item in data]
                self.audit_model.objects.bulk_create(audit_model_to_create)

                # meta_data is tuple of created audit item
                # and api response data (AuditChannel, {"snippet": ..., "statistics": ...})
                # Must create first to assign FK to meta model
                meta_data = zip(audit_model_to_create, data)
                meta_to_create = [
                    self.meta_model_instantiator(audit, data, language_mapping=self.language_mapping, country_mapping=self.country_mapping, category_mapping=self.category_mapping)
                    for audit, data in meta_data if audit
                ]
                self.audit_meta_model.objects.bulk_create(meta_to_create)

                # Create vetting items for all items retrieved in list
                all_audit_ids = list(existing_audit_ids) + [audit.id for audit in audit_model_to_create]
                audit_vetting_to_create = [
                    self.audit_vetting_model(**{"audit": audit_processor, self.id_field: _id})
                    for _id in all_audit_ids
                ]
                self.audit_vetting_model.objects.bulk_create(audit_vetting_to_create)
            except Exception:
                logger.exception("Error generating audit items")

    @staticmethod
    def instantiate_audit_model(data, id_field, audit_model, params=None):
        try:
            audit_params = {
                id_field: data["id"],
                f"{id_field}_hash": get_hash_name(data["id"])
            }
            if params:
                audit_params.update(params)
            audit_item = audit_model(**audit_params)
        except KeyError:
            audit_item = None
        return audit_item

    @staticmethod
    def instantiate_video_meta_model(audit_item, data, *_, language_mapping=None, category_mapping=None, **__):
        snippet = data.get("snippet", {})
        stats = data.get("statistics", {})
        status = data.get("status", {})
        content_details = data.get("contentDetails", {})
        meta_params = {
            "video": audit_item,
            "name": snippet.get("title"),
            "description": snippet.get("description"),
            "publish_date": snippet.get("publishedAt"),
            "keywords": ",".join(snippet["tags"]) if snippet.get("tags") else None,
            "category_id": snippet.get("categoryId"),
            "views": stats.get("viewCount", 0),
            "likes": stats.get("likeCount", 0),
            "dislikes": stats.get("dislikeCount", 0),
            "made_for_kids": status.get("madeForKids"),
            "default_audio_language": snippet.get("defaultAudioLanguage"),
            "duration": content_details.get("duration"),
            "age_restricted": content_details.get("contentRating", {}).get("ytRating", None) == "ytAgeRestricted"
        }
        text = meta_params["name"] or "" + meta_params["description"] or "" + meta_params["keywords"] or ""
        detected_language = BrandSafetyUtils.get_language(text)
        meta_params["language"] = language_mapping.get(detected_language)
        meta_params["default_audio_language"] = language_mapping.get(meta_params["default_audio_language"])
        if meta_params["category_id"]:
            category_id = meta_params["category_id"]
            category = category_mapping.get(category_id)
            if not category:
                category, _ = AuditCategory.objects.get_or_create(category=category_id)
                category_mapping[category_id] = category
            meta_params["category_id"] = category.id

        meta_item = AuditVideoMeta(**meta_params)
        return meta_item

    @staticmethod
    def instantiate_channel_meta_model(audit_item, data, *_, language_mapping, country_mapping, **__):
        snippet = data.get("snippet", {})
        stats = data.get("statistics", {})
        branding = data.get("brandingSettings", {}).get("channel", {})
        meta_params = {
            "channel": audit_item,
            "name": branding.get("title") or snippet.get("title"),
            "description": branding.get("description") or snippet.get("description"),
            "keywords": branding.get("keywords") or snippet.get("keywords"),
            "default_language": branding.get("defaultLanguage"),
            "country": branding.get("country") or snippet.get("country"),
            "subscribers": int(stats["subscriberCount"]) if stats.get("subscriberCount") else 0,
            "view_count": int(stats["viewCount"]) if stats.get("viewCount") else 0,
            "video_count": int(stats["videoCount"]) if stats.get("videoCount") else None,
        }
        text = meta_params["name"] or "" + meta_params["description"] or "" + meta_params["keywords"] or ""
        detected_language = BrandSafetyUtils.get_language(text)
        meta_params["default_language"] = language_mapping.get(detected_language)
        meta_params["language"] = language_mapping.get(detected_language)
        meta_params["country"] = country_mapping.get(meta_params["country"])

        meta_item = AuditChannelMeta(**meta_params)
        return meta_item

    @staticmethod
    def retrieve_youtube(func, item_ids):
        """
        Retrieve from Youtube Data API with YoutubeAPIConnector method
        :param func: YoutubeAPIConnector method
        :param item_ids: list
            youtube_part = "id,snippet,statistics,..."
        :return: list
        """
        data = []
        for ids in chunks_generator(item_ids, size=50):
            ids_term = ",".join(ids)
            response = func(ids_term)["items"]
            data.extend(response)
        return data

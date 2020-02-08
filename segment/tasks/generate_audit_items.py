from audit_tool.models import get_hash_name
from brand_safety.auditors.utils import AuditUtils
from es_components.constants import Sections

from audit_tool.models import AuditChannelMeta
from audit_tool.models import AuditCountry
from audit_tool.models import AuditLanguage
from audit_tool.models import AuditProcessor
from utils.utils import chunks_generator
from utils.youtube_api import YoutubeAPIConnector


"""
1. Get the ids of Channel / Videos that we need to create Audit models for
2. Use those ids to create JUST audit models
3. Create vetting items for ALL ids

"""

BATCH_SIZE = 200


def generate_audit_items(item_ids, segment, data_field="video"):
    """
    Generate audit items for segment vetting process
    If segment is provided, all audit kwargs are not required as segment has all attributes

    :param item_ids: list -> str
    :param segment: CustomSegment
    :param data_field: str -> video | channel
    :param audit: AuditProcessor
    :param audit_model: AuditChannel | AuditVideo
    :param audit_meta_model: AuditVideoMeta | AuditChannelMeta
    :param audit_vetting_model: AuditChannelVet
    :param es_manager: VideoManager | ChannelManager
    :return:
    """
    language_mapping = get_language_mapping()
    country_mapping = get_country_mapping()
    youtube_connector = YoutubeAPIConnector()
    if data_field == "video":
        audit_processor_type = 1
        youtube_connector = youtube_connector.obtain_videos
        meta_model_instantiator = instantiate_video_meta_model
    elif data_field == "channel":
        audit_processor_type = 2
        youtube_connector = youtube_connector.obtain_channels
        meta_model_instantiator = instantiate_channel_meta_model
    else:
        raise ValueError(f"Unsupported data field: {data_field}")
    audit, created = AuditProcessor.objects.get_or_create(id=segment.audit_id, defaults={
        "audit_type": audit_processor_type,
        "source": 1
    })
    if created:
        segment.audit_id = audit.id
        segment.save()
    audit_model = segment.audit_utils.model
    audit_meta_model = segment.audit_utils.meta_model
    audit_vetting_model = segment.audit_utils.vetting_model
    es_manager = segment.es_manager
    es_manager.sections = (Sections.GENERAL_DATA, Sections.STATS)

    # video_id, channel_id
    id_field = data_field + "_id"
    for batch in chunks_generator(item_ids, size=BATCH_SIZE):
        batch = list(batch)
        # Get the ids of audit Channel / Video items we need to create
        filter_query = {f"{id_field}__in": batch}
        existing_audit_items = list(audit_model.objects.filter(**filter_query))
        existing_audit_ids = [getattr(item, id_field) for item in existing_audit_items]

        # Generate audit / audit meta items
        to_create_ids = set(batch) - set(existing_audit_ids)
        data = []
        for ids in chunks_generator(to_create_ids, size=50):
            response = youtube_connector(",".join(ids))["items"]
            data.extend(response)
        audit_model_to_create = [instantiate_audit_model(item, id_field, audit_model) for item in data]
        audit_model.objects.bulk_create(audit_model_to_create)

        # meta_data is tuple of created audit item
        # and api response data (AuditChannel, {"snippet": ..., "statistics": ...})
        # Must create first to assign FK to meta model
        meta_data = zip(audit_model_to_create, data)
        meta_to_create = [meta_model_instantiator(audit, data, language_mapping, country_mapping) for audit, data in meta_data]
        audit_meta_model.objects.bulk_create(meta_to_create)

        all_audit_items = list(existing_audit_items) + audit_model_to_create
        audit_vetting_to_create = [audit_vetting_model(**{"audit": audit, data_field: obj}) for obj in all_audit_items]
        audit_vetting_model.objects.bulk_create(audit_vetting_to_create)


def instantiate_audit_model(data, id_field, audit_model):
    audit_params = {
        id_field: data["id"],
        f"{id_field}_hash": get_hash_name(data["id"])
    }
    audit_item = audit_model(**audit_params)
    return audit_item


def instantiate_video_meta_model():
    pass


def instantiate_channel_meta_model(audit_item, data, language_mapping, country_mapping):
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
        "subscribers": int(stats["subscriberCount"]) if stats.get("subscriberCount") else None,
        "view_count": int(stats["viewCount"]) if stats.get("viewCount") else None,
        "video_count": int(stats["videoCount"]) if stats.get("videoCount") else None,
    }
    text = meta_params["name"] or "" + meta_params["description"] + meta_params["keywords"] or ""
    detected_language = AuditUtils.get_language(text)
    meta_params["language"] = language_mapping.get(detected_language)
    meta_params["country"] = country_mapping.get(meta_params["country"])

    meta_item = AuditChannelMeta(**meta_params)
    return meta_item


def get_language_mapping():
    mapping = {
        item.language: item for item in AuditLanguage.objects.all()
    }
    return mapping


def get_country_mapping():
    mapping = {
        item.country: item for item in AuditCountry.objects.all()
    }
    return mapping

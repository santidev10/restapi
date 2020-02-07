from django.db import transaction

from audit_tool.models import get_hash_name
from utils.utils import chunks_generator

"""
1. Get the ids of Channel / Videos that we need to create Audit models for
2. Use those ids to create JUST audit models
3. Create vetting items for ALL ids

"""


def generate_audit_items(items, segment=None, data_field="video", audit=None, audit_model=None, audit_meta_model=None,
                         audit_vetting_model=None, es_manager=None):
    """
    Generate audit items for segment vetting process
    If segment is provided, all audit kwargs are not required as segment has all attributes

    :param items: list -> int | str
    :param segment: CustomSegment
    :param data_field: str -> video | channel
    :param audit: AuditProcessor
    :param audit_model: AuditChannel | AuditVideo
    :param audit_meta_model: AuditVideoMeta | AuditChannelMeta
    :param audit_vetting_model: AuditChannelVet
    :param es_manager: VideoManager | ChannelManager
    :return:
    """
    if not items:
        return

    if segment is None:
        if audit is None and audit_model is None and audit_meta_model is None and audit_vetting_model is None and es_manager is None:
            raise ValueError("If segment is not provided, audit, audit_model, audit_meta_model, "
                             "audit_vetting_model, and es_manager parameters are required.")
    else:
        audit = segment.audit
        audit_model = segment.audit_utils.model
        audit_meta_model = segment.audit_utils.meta_model
        audit_vetting_model = segment.audit_utils.vetting_model
        es_manager = segment.es_manager

    audit_items = []
    audit_meta_items = []
    audit_vet_items = []
    audit_items_to_create = []
    audit_meta_to_create = []

    id_field = data_field + "_id"
    for batch in chunks_generator(items, size=10000):
        if type(batch[0]) is str:
            item_ids = batch
        else:
            # Assume items are Elasticsearch documents
            item_ids = [doc.main.id for doc in batch]

        # Get the ids of audit Channel / Video items we need to create
        filter_query = {f"{id_field}__in": item_ids}
        existing_audit_items = audit_model.objects.filter(**filter_query)
        existing_audit_ids = set(existing_audit_items.values_list(id_field, flat=True))

        # Generate audit / audit meta items
        to_create_ids = set(batch) - existing_audit_ids

        if type(batch[0]) is str:
            # Retrieve data for audit item instantiations
            data = es_manager.get(to_create_ids)
        else:
            data = [doc for doc in batch if doc.main.id in to_create_ids]

        # Instantiate audit vetting items for transaction
        for doc in data:
            item_id = doc.main.id
            audit_params = {
                id_field: item_id,
                f"{id_field}_hash": get_hash_name(item_id)
            }
            audit_item = audit_model(**audit_params)
            meta_params = {
                data_field: audit_item,

            }
            audit_meta = audit_meta_model(**meta_params)
            audit_items_to_create.append(audit_item)
            audit_meta_to_create.append(audit_meta)

        # Create audit vetting items
        with transaction.atomic():
            for _id in batch:
                audit_items.append(audit_model.get_or_create(_id))
                audit_meta_items.append(audit_meta_model.objects.get_or_create(**{data_field: _id}))
                audit_vet_items.append(
                    audit_vetting_model.objects.get_or_create(**{data_field: _id, "audit": audit}))

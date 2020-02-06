from django.db import transaction

from audit_tool.models import get_hash_name
from utils.utils import chunks_generator


def generate_audit_items(item_ids, segment=None, audit=None, audit_model=None, audit_meta_model=None,
                         audit_vetting_model=None, es_manager=None, data_field="video"):

    if segment is None:
        if audit is None and audit_model is None and audit_meta_model is None and audit_vetting_model is None and es_manager:
            raise ValueError("If segment is not provided, audit, audit_model, audit_meta_model, "
                             "audit_vetting_model, and es_manager parameters are required.")
    else:
        audit = segment.audit
        es_manager = segment.es_manager
        audit_model, audit_meta_model, audit_vetting_model = segment.audit_models

    audit_items = []
    audit_meta_items = []
    audit_vet_items = []
    audit_items_to_create = []
    audit_meta_to_create = []

    id_field = data_field + "_id"
    for batch in chunks_generator(item_ids, size=10000):
        filter_query = {f"{id_field}__in": batch}
        existing_audit_items = audit_model.objects.filter(**filter_query)
        existing_audit_ids = set(existing_audit_items.values_list(id_field, flat=True))
        # Just generate audit / audit meta items, NOT vetting items
        to_create_ids = set(batch) - existing_audit_ids
        docs = es_manager.get(to_create_ids)

        for doc in docs:
            item_id = doc.main.id
            audit_params = {
                id_field: item_id,
                f"{id_field}_hash": get_hash_name(item_id)
            }
            item = audit_model(**audit_params)
            meta_params = {
                data_field: item,

            }

            audit_items_to_create.append(audit_model())


            audit_items_to_create.append(audit_model())

    for batch in chunks_generator(item_ids, size=1000):
        with transaction.atomic():
            for _id in batch:
                audit_items.append(audit_model.get_or_create(_id))
                audit_meta_items.append(audit_meta_model.objects.get_or_create(**{data_field: _id}))
                audit_vet_items.append(
                    audit_vetting_model.objects.get_or_create(**{data_field: _id, "audit": audit}))

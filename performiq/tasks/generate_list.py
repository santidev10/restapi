import csv
import tempfile

from django.conf import settings

from es_components.constants import SortDirections
from es_components.constants import SUBSCRIBERS_FIELD
from es_components.models import Channel
from segment.api.serializers import CTLParamsSerializer

from segment.utils.query_builder import SegmentQueryBuilder
from segment.utils.bulk_search import bulk_search

EXPORT_LIMIT = 20000
PERFORMANCE_RESULT_KEY = "performance"
CONTEXTUAL_RESULT_KEY = "contextual"
SUITABILITY_RESULT_KEY = "suitability"


def create_recommended_list(source_ids, iq_campaign):
    params_serializer = CTLParamsSerializer(data=iq_campaign.params)
    params_serializer.is_valid()
    filename = tempfile.mkstemp(dir=settings.TEMPDIR)[1]

    if len(source_ids) < EXPORT_LIMIT:
        query_builder = SegmentQueryBuilder(params_serializer.validated_data)
        sort = [{SUBSCRIBERS_FIELD: {"order": SortDirections.DESCENDING}}]
        for batch in bulk_search(Channel, query_builder.query_body, sort=sort, cursor_field=SUBSCRIBERS_FIELD):
            source_ids.extend(doc.main.id for doc in batch)
            if len(source_ids) >= EXPORT_LIMIT:
                break

    source_ids = source_ids[:20000]
    with open(filename, mode="w") as file:
        writer = csv.writer(file)
        writer.writerows([channel_id] for channel_id in source_ids)


def create_wastage_list(iq_campaign):
    filename = tempfile.mkstemp(dir=settings.TEMPDIR)[1]
    iq_channels = iq_campaign.channels.filter(clean=False)
    rows = [
        [c.channel_id, c.results[PERFORMANCE_RESULT_KEY]["pass"], c.results[CONTEXTUAL_RESULT_KEY]["pass"],
         c.results[SUITABILITY_RESULT_KEY]["pass"]] for c in iq_channels
    ]
    with open(filename, mode="w") as file:
        writer = csv.writer(file)
        writer.writerows(rows)

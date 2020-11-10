import csv
import logging
import os
import tempfile

from django.conf import settings

from es_components.constants import MAIN_ID_FIELD
from es_components.constants import SortDirections
from es_components.constants import SUBSCRIBERS_FIELD
from es_components.models import Channel
from performiq.models import IQCampaignChannel
from segment.api.serializers import CTLParamsSerializer

from segment.utils.query_builder import SegmentQueryBuilder
from segment.utils.bulk_search import bulk_search
from segment.utils.utils import get_content_disposition
from performiq.tasks.utils.s3_exporter import PerformS3Exporter
from performiq.models.constants import EXPORT_RESULTS_KEYS
from utils.datetime import now_in_default_tz

EXPORT_LIMIT = 20000
PERFORMANCE_RESULT_KEY = "performance"
CONTEXTUAL_RESULT_KEY = "contextual"
SUITABILITY_RESULT_KEY = "suitability"


logger = logging.getLogger(__name__)


def generate_exports(iq_campaign):
    exporter = PerformS3Exporter()
    recommended_fp = tempfile.mkstemp(dir=settings.TEMPDIR)[1]
    wastage_fp = tempfile.mkstemp(dir=settings.TEMPDIR)[1]
    recommended_s3_key = with_cleanup(create_recommended_export, iq_campaign, exporter, recommended_fp)
    wastage_s3_key = with_cleanup(create_wastage_export, iq_campaign, exporter, wastage_fp)
    iq_campaign.results.update({
        EXPORT_RESULTS_KEYS.RECOMMENDED_EXPORT_FILENAME: recommended_s3_key,
        EXPORT_RESULTS_KEYS.WASTAGE_EXPORT_FILENAME: wastage_s3_key
    })
    iq_campaign.save(update_fields=["results"])


def create_recommended_export(iq_campaign, exporter, filepath):
    clean_ids = list(IQCampaignChannel.objects.filter(iq_campaign=iq_campaign, clean=True)
                     .values_list("channel_id", flat=True)[:EXPORT_LIMIT])
    params_serializer = CTLParamsSerializer(data=iq_campaign.params)
    params_serializer.is_valid()

    if len(clean_ids) < EXPORT_LIMIT:
        query_builder = SegmentQueryBuilder(params_serializer.validated_data)
        sort = [{SUBSCRIBERS_FIELD: {"order": SortDirections.DESCENDING}}]
        for batch in bulk_search(Channel, query_builder.query_body, sort=sort, cursor_field=SUBSCRIBERS_FIELD,
                                 source=(MAIN_ID_FIELD,)):
            clean_ids.extend(doc.main.id for doc in batch)
            if len(clean_ids) >= EXPORT_LIMIT:
                break

    clean_ids = clean_ids[:20000]
    with open(filepath, mode="w") as file:
        writer = csv.writer(file)
        writer.writerows([channel_id] for channel_id in clean_ids)

    display_filename = f"{iq_campaign.campaign.name}_recommended_{now_in_default_tz().date()}.csv"
    s3_key = exporter.export_file(filepath, display_filename)
    return s3_key


def create_wastage_export(iq_campaign, exporter, filepath):
    iq_channels = iq_campaign.channels.filter(clean=False)
    rows = [
        [c.channel_id, c.results[PERFORMANCE_RESULT_KEY]["pass"], c.results[CONTEXTUAL_RESULT_KEY]["pass"],
         c.results[SUITABILITY_RESULT_KEY]["pass"]] for c in iq_channels
    ]
    with open(filepath, mode="w") as file:
        writer = csv.writer(file)
        writer.writerows(rows)

    display_filename = f"{iq_campaign.campaign.name}_wastage_{now_in_default_tz().date()}.csv"
    s3_key = exporter.export_file(filepath, display_filename)
    return s3_key


def with_cleanup(export_func, iq_campaign, exporter, filepath):
    result = None
    try:
        result = export_func(iq_campaign, exporter, filepath)
    except Exception:
        logger.exception(f"Error generating export with {export_func.__name__}")
    finally:
        os.remove(filepath)
    return result

import csv
import logging
import os
import tempfile
from typing import Callable

from django.conf import settings

from es_components.constants import LAST_VETTED_AT_MIN_DATE
from es_components.constants import MAIN_ID_FIELD
from es_components.constants import Sections
from es_components.constants import SortDirections
from es_components.constants import SUBSCRIBERS_FIELD
from es_components.query_builder import QueryBuilder
from es_components.models import Channel
from performiq.analyzers.constants import ANALYSIS_RESULT_SECTIONS
from performiq.api.serializers.query_serializer import IQCampaignQuerySerializer
from performiq.models import IQCampaign
from performiq.models import IQCampaignChannel
from segment.utils.bulk_search import bulk_search
from segment.utils.query_builder import SegmentQueryBuilder
from performiq.utils.s3_exporter import PerformS3Exporter
from performiq.models.constants import EXPORT_RESULTS_KEYS
from utils.datetime import now_in_default_tz

EXPORT_LIMIT = 20000

logger = logging.getLogger(__name__)


def generate_exports(iq_campaign: IQCampaign):
    """
    Main function to drive export results of PerformIQ analysis
    Calls export functions and saves results to IQCampaign results field
    :param iq_campaign: IQCampaign being processed
    :return: None
    """
    exporter = PerformS3Exporter()
    # Prepare paths to generate exports
    recommended_fp = tempfile.mkstemp(dir=settings.TEMPDIR)[1]
    wastage_fp = tempfile.mkstemp(dir=settings.TEMPDIR)[1]
    recommended_s3_key, total_recommended = with_cleanup(create_recommended_export, iq_campaign, exporter, recommended_fp)
    wastage_s3_key, total_wastage = with_cleanup(create_wastage_export, iq_campaign, exporter, wastage_fp)

    results = {
        EXPORT_RESULTS_KEYS.RECOMMENDED_EXPORT_FILENAME: recommended_s3_key,
        EXPORT_RESULTS_KEYS.WASTAGE_EXPORT_FILENAME: wastage_s3_key,
        "recommended_count": total_recommended,
        "wastage_count": total_wastage,
    }
    return results


def create_recommended_export(iq_campaign: IQCampaign, exporter: PerformS3Exporter, filepath: str) -> tuple:
    """
    Generate and export csv file of all placement ids that passed analyze as well as additional placement with
        similar qualities
    :param iq_campaign: IQCampaign export is being generated for
    :param exporter: PerformS3Exporter instantiation
    :param filepath: Filepath of file being used to generate export
    :return: str file key stored on S3
    """
    analyzed_clean = set(IQCampaignChannel.objects.filter(iq_campaign=iq_campaign, clean=True)
                         .distinct()
                         .values_list("channel_id", flat=True)[:EXPORT_LIMIT])
    clean_ids = list(analyzed_clean)
    if len(clean_ids) < EXPORT_LIMIT:
        params_serializer = IQCampaignQuerySerializer(data=iq_campaign.params)
        params_serializer.is_valid()
        query = SegmentQueryBuilder(params_serializer.validated_data).query_body
        query &= QueryBuilder().build().must().range().field(f"{Sections.TASK_US_DATA}.last_vetted_at")\
            .gte(LAST_VETTED_AT_MIN_DATE).get()
        sort = [{SUBSCRIBERS_FIELD: {"order": SortDirections.DESCENDING}}]
        for batch in bulk_search(Channel, query, sort=sort, cursor_field=SUBSCRIBERS_FIELD, batch_size=5000,
                                 source=(MAIN_ID_FIELD, f"{Sections.STATS}.subscribers"),
                                 include_cursor_exclusions=True):
            ids = [doc.main.id for doc in batch if doc.main.id not in analyzed_clean]
            clean_ids.extend(ids)
            if len(clean_ids) >= EXPORT_LIMIT:
                break

    clean_ids = clean_ids[:EXPORT_LIMIT]
    with open(filepath, mode="w") as file:
        writer = csv.writer(file)
        writer.writerow(["URL"])
        writer.writerows([f"https://www.youtube.com/channel/{channel_id}"] for channel_id in clean_ids)

    display_filename = f"{iq_campaign.name}_recommended_{now_in_default_tz()}.csv"
    s3_key = exporter.export_file(filepath, display_filename)
    return s3_key, len(clean_ids)


def create_wastage_export(iq_campaign, exporter, filepath):
    """
    Generate and export file to S3 of all placements that failed analysis
    :param iq_campaign: IQCampaign export is being generated for
    :param exporter: PerformS3Exporter instantiation
    :param filepath: Filepath of file being used to generate export
    :return: str file key stored on S3
    """
    iq_channels = iq_campaign.channels.filter(clean=False).distinct()
    rows = []
    for iq in iq_channels:
        # Get failure result for each section in analysis
        failed_values = [get_failed_repr(iq.results[key]["passed"]) for key in ANALYSIS_RESULT_SECTIONS]
        rows.append([f"https://www.youtube.com/channel/{iq.channel_id}", *failed_values])
    with open(filepath, mode="w") as file:
        writer = csv.writer(file)
        writer.writerow(["URL", "performance failed", "contextual failed", "suitability failed"])
        writer.writerows(rows)

    display_filename = f"{iq_campaign.name}_wastage_{now_in_default_tz()}.csv"
    s3_key = exporter.export_file(filepath, display_filename)
    return s3_key, len(rows)


def get_failed_repr(passed):
    return "x" if passed is False else ""


def with_cleanup(export_func: Callable, iq_campaign: IQCampaign, exporter: PerformS3Exporter, filepath: str):
    """
    Handle export creation by calling export_func with deletion of filepath after export_func returns.
        export_func should generate and export file to S3 as this function will delete the filepath used
    :param export_func: create_recommended_export or create_wastage_export
    :param iq_campaign: IQCampaign being exported
    :param exporter: PerformS3Exporter instantiation
    :param filepath: Filepath to write export results
    :return:
    """
    result = None
    try:
        result = export_func(iq_campaign, exporter, filepath)
    except Exception:
        logger.exception(f"Error generating export with {export_func.__name__}")
    finally:
        os.remove(filepath)
    return result

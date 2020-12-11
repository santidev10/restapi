import csv
import logging
import os
import tempfile

from django.conf import settings

from performiq.analyzers.constants import COERCE_FIELD_FUNCS
from performiq.models.constants import AnalysisFields
from performiq.utils.s3_exporter import PerformS3Exporter
from performiq.utils.constants import CSVFieldTypeEnum

logger = logging.getLogger(__name__)

# Create a mapping of AnalysisFields to csv data columns
CSV_HEADER_MAPPING = {
    CSVFieldTypeEnum.URL.value: "url",
    CSVFieldTypeEnum.VIEW_RATE.value: AnalysisFields.VIDEO_VIEW_RATE,
    CSVFieldTypeEnum.AVERAGE_CPV.value: AnalysisFields.CPV,
    CSVFieldTypeEnum.AVERAGE_CPM.value: AnalysisFields.CPM,
    CSVFieldTypeEnum.CTR.value: AnalysisFields.CTR,
    CSVFieldTypeEnum.VIDEO_PLAYED_TO_100_RATE.value: AnalysisFields.VIDEO_QUARTILE_100_RATE,
}


def get_csv_data(iq_campaign):
    s3 = PerformS3Exporter()
    csv_fp = tempfile.mkstemp(dir=settings.TEMPDIR)[1]
    try:
        csv_s3_key = iq_campaign.params["csv_s3_key"]
        csv_column_mapping = iq_campaign.params["csv_column_mapping"]
        column_letter_to_metric_names = {value: key for key, value in csv_column_mapping.items()}
        s3.download_file(csv_s3_key, csv_fp)
        # User might incorrectly upload multiple rows of the placement id. Use data from first instance
        seen_placement_ids = set()
        with open(csv_fp, mode="r") as file:
            reader = csv.reader(file)
            for row in reader:
                # Skip non data rows
                if all(r.replace(" ", "").isalpha() for r in row) or not row or "channel" not in "".join(row):
                    try:
                        next(reader)
                    except StopIteration:
                        break
                    else:
                        continue
                # Map column letters to metric names. csv_column_mapping will be a dict of column letter
                # to metric name e.g. {"A": "impressions", "B": "video views", ...}
                # Then use sorted column letters to get column value e.g. A = 0, B = 1, ... to assign with metric_name
                raw_values_by_metric_name = {}
                for column_letter in sorted([key for key in column_letter_to_metric_names.keys() if key is not None]):
                    column_index = ord(column_letter.lower()) - 97
                    metric_name = column_letter_to_metric_names[column_letter]
                    raw_values_by_metric_name[metric_name] = row[column_index]

                placement_seen = False
                # Map metric names to AnalysisFields that is used for all data sources
                mapped_metrics = {}
                for metric_name, metric_value in raw_values_by_metric_name.items():
                    if metric_name == CSVFieldTypeEnum.URL.value:
                        placement_id = COERCE_FIELD_FUNCS["channel_url"](metric_value)
                        if placement_id in seen_placement_ids or len(placement_id) != 24 or placement_id[:2] != "UC":
                            placement_seen = True
                            break
                        mapped_metrics[AnalysisFields.CHANNEL_ID] = placement_id
                        seen_placement_ids.add(placement_id)
                    else:
                        mapped_key = CSV_HEADER_MAPPING.get(metric_name, metric_name)
                        coercer = COERCE_FIELD_FUNCS.get(mapped_key)
                        mapped_value = coercer(metric_value) if coercer else metric_value
                        mapped_metrics[mapped_key] = mapped_value
                if placement_seen is True:
                    continue
                yield mapped_metrics
    except Exception:
        logger.exception(f"Error processing PerformIQ csv file. S3 file key: {s3}")
    finally:
        os.remove(csv_fp)



import csv
import logging
import os

from performiq.analyzers.constants import COERCE_FIELD_FUNCS
from performiq.models.constants import AnalysisFields

logger = logging.getLogger(__name__)

# Create a mapping of AnalysisFields to csv data columns
CSV_HEADER_MAPPING = {
    "url": "url",
    "view rate": AnalysisFields.VIDEO_VIEW_RATE,
    "cpv": AnalysisFields.CPV,
    "cpm": AnalysisFields.CPM,
    "ctr": AnalysisFields.CTR,
}


def get_csv_data(iq_campaign):
    try:
        csv_s3_key = iq_campaign.params["csv_s3_key"]
        csv_column_mapping = iq_campaign.params["csv_column_mapping"]
        with open("with_headers.csv", mode="r") as file:
            reader = csv.reader(file)
            for row in reader:
                # Skip if header row
                if all(r.replace(" ", "").isalpha() for r in row) or not row:
                    row = next(reader)

                # Map column letters to metric names. csv_column_mapping will be a dict of column letter
                # to metric name e.g. {"A": "impressions", "B": "video views", ...}
                # Then use sorted column letters to get column value e.g. A = 0, B = 1, ... to assign with metric_name
                raw_values_by_metric_name = {}
                for index, column_letter in enumerate(sorted(csv_column_mapping.keys())):
                    metric_name = csv_column_mapping[column_letter]
                    raw_values_by_metric_name[metric_name] = row[index]

                # Map metric names to AnalysisFields that is used for all data sources
                mapped_metrics = {}
                for metric_name, metric_value in raw_values_by_metric_name.items():
                    if metric_name == "url":
                        placement_id = COERCE_FIELD_FUNCS["channel_url"](metric_value)
                        mapped_metrics[AnalysisFields.CHANNEL_ID] = placement_id
                    else:
                        mapped_key = CSV_HEADER_MAPPING.get(metric_name, metric_name)
                        coercer = COERCE_FIELD_FUNCS.get(mapped_key)
                        mapped_value = coercer(metric_value) if coercer else metric_value
                        mapped_metrics[mapped_key] = mapped_value
                yield mapped_metrics
    except Exception:
        logger.exception(f"Error processing PerformIQ csv file. S3 file key: {s3}")
    finally:
        print('done')
        # os.remove(csv_fp)



import csv
import logging
import os
import tempfile

from django.conf import settings

from performiq.analyzers.constants import COERCE_FIELD_FUNCS
from performiq.tasks.utils.s3_exporter import PerformS3Exporter
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


# def get_csv_data(s3_key):
#     s3 = PerformS3Exporter()
#     csv_fp = tempfile.mkstemp(dir=settings.TEMPDIR)[1]
#     try:
#         s3.download_file(s3_key, csv_fp)
#         with open(csv_fp, mode="r") as file:
#             reader = csv.DictReader(file)
#             columns = reader.fieldnames
#             for row in reader:
#                 mapped = {
#                     CSV_HEADER_MAPPING.get(key, key): row[key] for key in columns
#                 }
#                 yield mapped
#     except Exception:
#         logger.exception(f"Error processing PerformIQ csv file. S3 file key: {s3}")
#     finally:
#         os.remove(csv_fp)

def get_csv_data(s3_key):
    try:
        with open("with_headers.csv", mode="r") as file:
            reader = csv.DictReader(file)
            columns = reader.fieldnames
            for row in reader:
                curr_mapped = {}
                for column in columns:
                    raw_value = row[column]
                    if column == "url":
                        placement_id = COERCE_FIELD_FUNCS["channel_url"](raw_value)
                        curr_mapped[AnalysisFields.CHANNEL_ID] = placement_id
                    else:
                        mapped_key = CSV_HEADER_MAPPING.get(column, column)
                        coercer = COERCE_FIELD_FUNCS.get(mapped_key)
                        mapped_value = coercer(raw_value) if coercer else raw_value
                        curr_mapped[mapped_key] = mapped_value
                yield curr_mapped
    except Exception:
        logger.exception(f"Error processing PerformIQ csv file. S3 file key: {s3}")
    finally:
        os.remove(csv_fp)


import csv
import logging
import os
import tempfile

from django.conf import settings

from performiq.tasks.utils.s3_exporter import PerformS3Exporter
from performiq.models.constants import CampaignDataFields

CSV_HEADER_MAPPING = {
    "impressions": CampaignDataFields.IMPRESSIONS,
}

logger = logging.getLogger(__name__)

# Create a mapping of CampaignDataFields to csv data fields

def get_csv_data(s3_key):
    # Get s3 key from s3
    s3 = PerformS3Exporter()
    csv_fp = tempfile.mkstemp(dir=settings.TEMPDIR)[1]
    try:
        s3.download_file(s3_key, csv_fp)
        with open(csv_fp, mode="r") as file:
            reader = csv.DictReader(file)
            columns = reader.fieldnames
            for row in reader:
                mapped = {
                    CSV_HEADER_MAPPING.get(key, key): row[key] for key in columns
                }
                yield mapped
    except Exception:
        logger.exception(f"Error processing PerformIQ csv file. S3 file key: {s3}")
    finally:
        os.remove(csv_fp)


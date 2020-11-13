import csv
import os
import tempfile

from django.conf import settings

from performiq.tasks.utils.s3_exporter import PerformS3Exporter
from performiq.models.constants import CampaignDataFields


# Create a mapping of CampaignDataFields to csv data fields

def get_csv_data(s3_key):
    # Get s3 key from s3
    s3 = PerformS3Exporter()
    csv_fp = tempfile.mkstemp(dir=settings.TEMPDIR)[1]
    try:
        s3.download_file(s3_key, csv_fp)
        with open(csv_fp, mode="r") as file:
            reader = csv.reader(file)
            for row in reader:
                mapped = {}
                yield mapped
    except Exception as e:
        pass
    finally:
        os.remove(csv_fp)


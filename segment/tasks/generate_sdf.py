import csv
import os

from django.conf import settings
from uuid import uuid4

from segment.models.constants import Results
from segment.models import CustomSegment
from performiq.models import OAuthAccount
from utils.dv360_api import DV360Connector


# DV360 API AdGroup SDF downloads contain these columns which actually break uploading SDFs in the DV360 UI.
REMOVE_COLUMNS = [
    "Placement Targeting - Popular Content - Include"
]
PLACEMENTS_INCLUSION_KEY = "Placement Targeting - YouTube Channels - Include"


def generate_sdf(ctl_id):
    ctl = CustomSegment.objects.get(id=ctl_id)
    oauth_account = OAuthAccount.objects.get(email="kenneth.oh@channelfactory.com", oauth_type=1)
    dv = DV360Connector(oauth_account.token, oauth_account.refresh_token)
    target_dir = f"{settings.TEMPDIR}/sdf/{uuid4()}"
    os.mkdir(target_dir)
    ad_group_sdf_fp = dv.get_ad_group_sdf_report("1878225", "18913942", target_dir)

    # SDF placements must be delimited by ;
    urls = "; ".join(ctl.s3.get_extract_export_ids())
    output_fp = target_dir + "/output.csv"
    # Edit sdf file with placements from ctl
    with open(ad_group_sdf_fp, "r") as source, \
            open(output_fp, "w") as dest:
        reader = csv.reader(source)
        writer = csv.writer(dest)

        header = next(reader)
        placements_idx = header.index(PLACEMENTS_INCLUSION_KEY)
        remove_idx = []
        # Find indexes of erroneous columns to remove from data rows
        for col in REMOVE_COLUMNS:
            try:
                i = header.index(col)
                header.pop(i)
                remove_idx.append(i)
            except ValueError:
                pass

        writer.writerow(header)
        for row in reader:
            # First update placements with ctl results
            row[placements_idx] = urls
            # Remove columns that SDF upload will not accept
            _remove_error_fields(row, remove_idx)
            writer.writerow(row)

    content_disposition = ctl.s3.get_content_disposition(f"{ctl.title}_{ctl.params('The line item')}_SDF_AdGroups.csv")
    s3_key = f"{uuid4()}.csv"
    ctl.s3.export_file_to_s3(output_fp, s3_key, extra_args=dict(ContentDisposition=content_disposition))
    ctl.update_statistics(Results.DV360, Results.EXPORT_FILENAME, s3_key)


def _remove_error_fields(row, idx):
    for i in idx:
        row.pop(i)

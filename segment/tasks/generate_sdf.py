import csv
import os

from django.conf import settings
from uuid import uuid4

from segment.models import CustomSegment
from performiq.models import OAuthAccount
from utils.dv360_api import DV360Connector


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
        try:
            header = next(reader)
            writer.writerow(header)
        except StopIteration:
            pass
        else:
            placement_inclusion_index = header.index("Placement Targeting - YouTube Channels - Include")
            for row in reader:
                row[placement_inclusion_index] = urls
                writer.writerow(row)

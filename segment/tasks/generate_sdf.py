import csv
import os

from django.core.exceptions import ObjectDoesNotExist
from django.contrib.auth import get_user_model
from django.conf import settings
from uuid import uuid4

from oauth.models import DV360Advertiser
from oauth.models import AdGroup
from oauth.models import OAuthAccount
from saas import celery_app
from segment.models.constants import Results
from segment.models import CustomSegment
from segment.utils.send_export_email import send_export_email
from utils.dv360_api import DV360Connector
from utils.exception import retry

"""
DV360 API AdGroup SDF downloads contain these columns which break uploading SDFs in the DV360 UI.
Unknown why downloading SDF through api contains these columns but does not accept them when uploading SDF with these 
columns through the DV360 UI
"""
REMOVE_COLUMNS = [
    "Placement Targeting - Popular Content - Include"
]
PLACEMENTS_INCLUSION_KEY = "Placement Targeting - YouTube Channels - Include"


@celery_app.task
def generate_sdf_task(user_id: get_user_model().id,
                      segment_id: CustomSegment.pk,
                      advertiser_id: DV360Advertiser.id,
                      adgroup_ids: list[AdGroup.id]):
    """
    Generate Ad group SDF with CustomSegment data as Ad group placements
    :param user_id: User id requesting DV360 sync and email completion recipient
    :param segment_id: CustomSegment id
    :param advertiser_id: DV360Advertiser id parent
    :param adgroup_ids: LineItem id parents
    """
    try:
        user = get_user_model().objects.get(id=user_id)
        segment = CustomSegment.objects.get(id=segment_id)
        oauth_account = OAuthAccount.objects.get(email=user.email, oauth_type=1)
    except ObjectDoesNotExist:
        return
    connector = DV360Connector(oauth_account.token, oauth_account.refresh_token)
    target_dir = f"{settings.TEMPDIR}/sdf_{uuid4()}"
    os.mkdir(target_dir)
    adgroup_sdf_fp = get_adgroup_sdf(connector, advertiser_id, target_dir, adgroup_ids)

    # SDF placements must be delimited by ;
    urls = "; ".join(segment.s3.get_extract_export_ids())
    output_fp = target_dir + "/output.csv"
    # Edit sdf file with placements from segment
    with open(adgroup_sdf_fp, "r") as source, \
            open(output_fp, "w") as dest:
        reader = csv.reader(source)
        writer = csv.writer(dest)

        header = next(reader)
        # Detect the column where placements need to be added
        placements_idx = header.index(PLACEMENTS_INCLUSION_KEY)
        remove_idx = []
        # Find indexes of erroneous columns to remove from data rows. Unknown why downloading SDF through api contains
        # these columns but does not accept them when uploading SDF with these columns through the DV360 UI.
        for col in REMOVE_COLUMNS:
            try:
                i = header.index(col)
                header.pop(i)
                remove_idx.append(i)
            except ValueError:
                pass

        writer.writerow(header)
        for row in reader:
            # First update placements with segment results
            row[placements_idx] = urls
            # Remove columns that SDF upload will not accept
            _remove_error_fields(row, remove_idx)
            writer.writerow(row)
    finalize_results(segment, output_fp, user.email)


@retry(count=5, delay=10)
def get_adgroup_sdf(connector, advertiser_id, target_dir, adgroup_ids):
    adgroup_sdf_fp = connector.get_adgroup_sdf_report(advertiser_id, target_dir, adgroup_ids=adgroup_ids)
    return adgroup_sdf_fp


@retry(count=5, delay=10)
def finalize_results(segment: CustomSegment, result_fp: str, user_email: str):
    """
    Upload SDF result and notify user
    :param segment: CustomSegement instance
    :param result_fp: Filepath of completed SDF
    :param user_email: Completion recipient
    :return:
    """
    content_disposition = segment.s3.get_content_disposition(f"{segment.title}_SDF_AdGroups.csv")
    s3_key = f"{uuid4()}.csv"
    segment.s3.export_file_to_s3(result_fp, s3_key, extra_args=dict(ContentDisposition=content_disposition))
    segment.update_statistics(Results.DV360_SYNC_DATA, Results.EXPORT_FILENAME, s3_key)
    send_export_email(user_email, f"{segment.title}: DV360 Ad Groups SDF Download", segment.s3.generate_temporary_url(s3_key))


def _remove_error_fields(row, idx) -> None:
    """
    Helper function to pop erroneous columns defined in REMOVE_COLUMNS
    :param row: list
    :param idx: int
    """
    for i in idx:
        row.pop(i)

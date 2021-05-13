import csv
import os
import shutil

from django.db.models import F
from django.core.exceptions import ObjectDoesNotExist
from django.contrib.auth import get_user_model
from django.conf import settings
from uuid import uuid4

from administration.notifications import send_html_email
from audit_tool.models import AuditChannelProcessor
from audit_tool.models import AuditProcessor
from audit_tool.models import AuditVideoProcessor
from oauth.models import AdGroup
from oauth.models import DV360Advertiser
from oauth.models import OAuthAccount
from saas import celery_app
from segment.models.constants import Results
from segment.models import CustomSegment
from segment.models.constants import SegmentTypeEnum
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
INCLUSION_COLUMN = "Placement Targeting - YouTube {YT_TYPE} - Include"
PLACEMENTS_CHANNEL_INCLUSION_COLUMN = INCLUSION_COLUMN.replace("{YT_TYPE}", "Channels")
PLACEMENTS_VIDEO_INCLUSION_COLUMN = INCLUSION_COLUMN.replace("{YT_TYPE}", "Videos")

MAX_PLACEMENTS = 20000


@celery_app.task
def generate_sdf_segment_task(user_id: get_user_model().id, audit_id, segment_id: CustomSegment.pk,
                              advertiser_id: DV360Advertiser.id, adgroup_ids: list[AdGroup.id]):
    """
    Generate Ad group SDF with CustomSegment data as Ad group placements
        This task retrieves SDF data for the target Ad Groups from the DV360 API to ensure that the only resources
        being updated on the generated SDF are the placements of the target Ad groups.
    :param user_id: User id requesting DV360 sync and email completion recipient
    :param segment_id: CustomSegment id
    :param advertiser_id: DV360Advertiser parent id of ad groups
    :param adgroup_ids: AdGroup ids to update placements for with CTL data
    """
    try:
        user = get_user_model().objects.get(id=user_id)
        segment = CustomSegment.objects.get(id=segment_id)
        oauth_account = OAuthAccount.get_enabled(email=user.email, oauth_type=1).last()
    except (ObjectDoesNotExist, KeyError, IndexError):
        return
    connector = DV360Connector(oauth_account.token, oauth_account.refresh_token)
    # Prepare directory where SDF will be downloaded to as we must download SDF files as zip files
    target_dir = f"{settings.TEMPDIR}/sdf_{uuid4()}"
    os.mkdir(target_dir)
    # SDF uploads must match the data existing in DV360. Only the placements columns must be edited
    adgroup_sdf_fp = get_adgroup_sdf(connector, advertiser_id, target_dir, adgroup_ids)

    # SDF placements must be delimited by ;
    formatted = ";".join(_get_placements(segment, audit_id))

    output_fp = target_dir + "/output.csv"
    # Edit SDF file with placements from segment
    with open(adgroup_sdf_fp, "r") as source, \
            open(output_fp, "w") as dest:
        reader = csv.reader(source)
        writer = csv.writer(dest)

        header = next(reader)
        # Previous placements must be removed if the data type (video | channel) is changing
        if segment.segment_type == SegmentTypeEnum.VIDEO:
            add_column = PLACEMENTS_VIDEO_INCLUSION_COLUMN
            remove_column = PLACEMENTS_CHANNEL_INCLUSION_COLUMN
        else:
            add_column = PLACEMENTS_CHANNEL_INCLUSION_COLUMN
            remove_column = PLACEMENTS_VIDEO_INCLUSION_COLUMN

        # Detect the column where placements need to be added and removed
        placements_add_idx = header.index(add_column)
        placements_remove_idx = header.index(remove_column)
        remove_erroneous_idx = []
        # Find indexes of erroneous columns to remove from data rows. Unknown why downloading SDF through api contains
        # these columns but does not accept them when uploading SDF with these columns through the DV360 UI
        for col in REMOVE_COLUMNS:
            try:
                i = header.index(col)
                header.pop(i)
                remove_erroneous_idx.append(i)
            except ValueError:
                pass

        writer.writerow(header)
        for row in reader:
            # Update placements with filtered results
            row[placements_add_idx] = formatted
            row[placements_remove_idx] = ""
            _remove_error_fields(row, remove_erroneous_idx)
            writer.writerow(row)
    finalize_results(segment, output_fp, user.email, adgroup_ids)
    shutil.rmtree(target_dir)


@retry(count=5, delay=10)
def get_adgroup_sdf(connector, advertiser_id, target_dir, adgroup_ids):
    adgroup_sdf_fp = connector.get_adgroup_sdf_report(advertiser_id, target_dir, adgroup_ids=adgroup_ids)
    return adgroup_sdf_fp


@retry(count=5, delay=10)
def finalize_results(segment: CustomSegment, result_fp: str, user_email: str, adgroup_ids: list[int]):
    """
    Upload SDF result and notify user
    :param segment: CustomSegment instance
    :param result_fp: Filepath of completed SDF
    :param user_email: Completion recipient
    :param adgroup_ids: Target AdGroup ids to update placements for
    :return:
    """
    content_disposition = segment.s3.get_content_disposition(f"{segment.title}_SDF_AdGroups.csv")
    s3_key = f"{uuid4()}.csv"
    segment.s3.export_file_to_s3(result_fp, s3_key, extra_args=dict(ContentDisposition=content_disposition))
    segment.update_statistics(s3_key, Results.DV360_SYNC_DATA, Results.EXPORT_FILENAME, save=True)
    _send_email(user_email, segment, s3_key, adgroup_ids)


def _remove_error_fields(row, idx) -> None:
    """
    Helper function to pop erroneous columns defined in REMOVE_COLUMNS
    :param row: list
    :param idx: int
    """
    for i in idx:
        row.pop(i)


def _get_placements(segment: CustomSegment, audit_id: AuditProcessor.pk) -> list:
    """
    Get audited placements that are clean from audit created in SegmentDV360SyncAPIView
    SDF uploads must have valid placements (e.g. not deleted from Youtube) in order to be successful. This function
        iterates through the CTL export (ordered by largest placements decreasing) and checks if each placement
        was determined clean by the audit process as they have been checked if they exist on Youtube
    :param segment: CustomSegment id
    :param audit_id: AuditProcessor id
    :return: Semi colon (;) delimited placement ids
    """
    config = {
        int(SegmentTypeEnum.VIDEO): {
            "id_annotation": "video__video_id",
            "model": AuditVideoProcessor,
        },
        int(SegmentTypeEnum.CHANNEL): {
            "id_annotation": "channel__channel_id",
            "model": AuditChannelProcessor,
        },
    }
    audit_config = config[segment.segment_type]
    clean_ids = set(
        audit_config["model"].objects
            .filter(audit_id=audit_id)
            .annotate(yt_id=F(audit_config["id_annotation"]))
            .values_list("yt_id", flat=True)
    )
    ctl_ids = segment.s3.get_extract_export_ids()
    to_export = []
    for placement_id in ctl_ids:
        if placement_id in clean_ids:
            to_export.append(placement_id)
        if len(to_export) >= MAX_PLACEMENTS:
            break
    return to_export


def _send_email(recipient_email: str, segment: CustomSegment, s3_key: str, adgroup_ids: list[int]) -> None:
    """ Send notification email to user with completed results """
    extra_content = '\n\n<p style="margin: 10px">Campaign Selections</p>' \
                    + '<table style="margin-top: 0 !important;">' \
                    + "".join([f'<tr><td text-align: left;"><p style="display: list-item; margin: 0; text-align: left;">{ag.display_name}</p></td></tr>' for ag in
                               AdGroup.objects.filter(id__in=adgroup_ids)]) \
                    + "</table>"
    download_url = segment.s3.generate_temporary_url(s3_key, time_limit=3600 * 24)
    text_content = "<a href={download_url}>Click here to download</a>".format(download_url=download_url) + extra_content
    send_html_email(
        subject=f"ViewIQ: Your {segment.title} SDF File",
        to=recipient_email,
        text_header=f"Your {segment.title} SDF File is ready",
        text_content=text_content,
        from_email=settings.EXPORTS_EMAIL_ADDRESS
    )

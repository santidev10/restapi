import csv
import logging
import os
import tempfile

from django.conf import settings
from django.contrib.auth import get_user_model
from uuid import uuid4

from administration.notifications import send_html_email
from audit_tool.api.serializers.blocklist_serializer import BlocklistSerializer
from audit_tool.models import BlacklistItem
from audit_tool.utils.blocklist_exporter import BlocklistExporter
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.query_builder import QueryBuilder
from utils.utils import chunks_generator
from utils.datetime import now_in_default_tz

logger = logging.getLogger(__name__)


def export_blocklist_task(recipient_id: int, blocklist_type: int = 0):
    """
    Export blocklist to csv
    :param blocklist_type: int -> Enumeration of blocklist type
        0: channels
        1: videos
        2: channels and videos
    :return:
    """
    try:
        user = get_user_model().objects.get(id=recipient_id)
    except get_user_model().DoesNotExist():
        logger.exception(f"Unable to find user id: {recipient_id}")
        return
    # Map blocklist_type to {0, 1} for determining which documents should be exported using _blocklist_generators
    if blocklist_type == 2:
        blocklist_type = {0, 1}
    else:
        blocklist_type = {blocklist_type}

    fp = tempfile.mkstemp(dir=settings.TEMPDIR)[1]
    try:
        with open(fp, mode="w") as file:
            writer = csv.DictWriter(file, fieldnames=BlocklistSerializer.EXPORT_FIELDS)
            writer.writeheader()
            for batch in chunks_generator(_blocklist_generators(blocklist_type), 100):
                batch = list(batch)
                blacklist = {obj.item_id: obj for obj in
                             BlacklistItem.objects.filter(item_id__in=[doc.main.id for doc in batch])}
                context = {"blacklist_data": blacklist}
                serialized = BlocklistSerializer(batch, many=True, context=context).data
                writer.writerows(serialized)
        download_url = _export(fp)
        _send_email(download_url, user.email)
    except Exception as e:
        logger.exception(e)
    finally:
        os.remove(fp)


def _blocklist_generators(blocklist_types: set):
    """
    Create Elasticsearch scan generators depending on blocklist_types
    Set membership in blocklist_types determines if channels, videos, or both are returned for export
    :param blocklist_types: set members should only be {0}, {1}, or {0,1}
    :return:
    """
    sections = (Sections.GENERAL_DATA, Sections.TASK_US_DATA)
    query = QueryBuilder().build().must().term().field(f"{Sections.CUSTOM_PROPERTIES}.blocklist").value(True).get()
    if 0 in blocklist_types:
        channel_manager = ChannelManager(sections)
        for channel in channel_manager.scan(query):
            yield channel
            break

    if 1 in blocklist_types:
        video_manager = VideoManager(sections)
        for video in video_manager.scan(query):
            yield video


def _export(filepath):
    exporter = BlocklistExporter()
    today = now_in_default_tz().date()
    key = f"{uuid4()}_{today}.csv"
    with open(filepath, mode="rb") as file:
        exporter.export_object_to_s3(file, key, extra_args={
            "ContentDisposition": f"attachment;filename=Blocklist {today}.csv"})
    download_url = exporter.generate_temporary_url(key)
    return download_url


def _send_email(download_url, recipient_email):
    text_content = f"<a href={download_url}>Click here to download</a>"
    send_html_email(
        subject="Blocklist Export",
        to=[recipient_email],
        text_header="Your Blocklist Export is ready.",
        text_content=text_content,
        from_email=settings.EXPORTS_EMAIL_ADDRESS,
    )










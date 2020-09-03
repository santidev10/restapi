import csv
import logging
import os
import tempfile

from django.conf import settings
from uuid import uuid4

from administration.notifications import send_html_email
from audit_tool.api.serializers.blocklist_serializer import BlocklistSerializer
from audit_tool.models import BlacklistItem
from audit_tool.utils.blocklist_exporter import BlocklistExporter
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.query_builder import QueryBuilder
from saas import celery_app
from utils.utils import chunks_generator
from utils.datetime import now_in_default_tz

logger = logging.getLogger(__name__)


@celery_app.task
def export_blocklist_task(recipient_email: str, data_type: str):
    """
    Export blocklist to csv and email recipient
    :param recipient_email: str -> Email recipient
    :param data_type: str -> video or channel
    """

    fp = tempfile.mkstemp(dir=settings.TEMPDIR)[1]
    try:
        with open(fp, mode="w") as file:
            writer = csv.DictWriter(file, fieldnames=BlocklistSerializer.EXPORT_FIELDS)
            writer.writeheader()
            for batch in chunks_generator(_blocklist_generator(data_type), 100):
                batch = list(batch)
                blacklist = {obj.item_id: obj for obj in
                             BlacklistItem.objects.filter(item_id__in=[doc.main.id for doc in batch])}
                context = {"blacklist_data": blacklist}
                serialized = BlocklistSerializer(batch, many=True, context=context).data
                writer.writerows(serialized)
        download_url = _export(fp)
        _send_email(download_url, recipient_email)
    except Exception as e:
        logger.exception(e)
    finally:
        os.remove(fp)


def _blocklist_generator(doc_type: str) -> object:
    """
    Create Elasticsearch scan generator depending on doc_type
    :param doc_type: str -> video | channel
    :return: Video or Channel document
    """
    managers = dict(
        video=VideoManager,
        channel=ChannelManager,
    )
    sections = (Sections.GENERAL_DATA, Sections.TASK_US_DATA)
    query = QueryBuilder().build().must().term().field(f"{Sections.CUSTOM_PROPERTIES}.blocklist").value(True).get()
    manager = managers[doc_type]
    for doc in manager(sections).scan(query):
        yield doc


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










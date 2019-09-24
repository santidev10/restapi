from saas import celery_app
from django.conf import settings
from django.core.mail import EmailMessage

from es_components.constants import Sections
from es_components.managers import ChannelManager
from utils.es_components_api_utils import ExportDataGenerator
from utils.es_components_api_utils import ESQuerysetAdapter
from utils.es_components_exporter import ESDataS3Exporter
from utils.aws.export_context_manager import ExportContextManager
from channel.constants import TERMS_FILTER
from channel.constants import MATCH_PHRASE_FILTER
from channel.constants import RANGE_FILTER
from channel.constants import EXISTS_FILTER
from channel.constants import CHANNEL_CSV_HEADERS
from channel.api.serializers.channel_export import ChannelListExportSerializer



class ChannelListDataGenerator(ExportDataGenerator):
    serializer_class = ChannelListExportSerializer
    terms_filter = TERMS_FILTER
    range_filter = RANGE_FILTER
    match_phrase_filter = MATCH_PHRASE_FILTER
    exists_filter = EXISTS_FILTER
    queryset = ESQuerysetAdapter(ChannelManager((
        Sections.MAIN,
        Sections.GENERAL_DATA,
        Sections.STATS,
        Sections.ADS_STATS,
        Sections.BRAND_SAFETY,
    )))


@celery_app.task
def export_channels_data(query_params, export_name, user_emails, export_url):
    content_exporter = ExportContextManager(
        ChannelListDataGenerator(query_params),
        CHANNEL_CSV_HEADERS
    )
    ESDataS3Exporter.export_to_s3(content_exporter, export_name)

    # prepare E-mail
    subject = "Export Channels"
    body = f"File is ready for downloading.\n" \
           f"Please, go to {export_url} to download the report.\n" \
           f"NOTE: url to download report is valid during next 2 weeks\n"

    # E-mail
    from_email = settings.SENDER_EMAIL_ADDRESS
    bcc = []

    email = EmailMessage(
        subject=subject,
        body=body,
        from_email=from_email,
        to=user_emails,
        bcc=bcc,
    )
    email.send(fail_silently=False)
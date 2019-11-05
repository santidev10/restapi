import logging

from saas import celery_app
from django.conf import settings
from django.core.mail import EmailMessage

from es_components.constants import Sections
from es_components.managers import ChannelManager
from utils.es_components_api_utils import ExportDataGenerator
from utils.es_components_api_utils import ESQuerysetAdapter
from utils.es_components_exporter import ESDataS3Exporter
from utils.aws.export_context_manager import ExportContextManager
from highlights.api.views.channels import ORDERING_FIELDS
from highlights.api.views.channels import TERMS_FILTER
from channel.constants import CHANNEL_CSV_HEADERS
from channel.api.serializers.channel_export import ChannelListExportSerializer

logger = logging.getLogger(__name__)


class HighlightsChannelListDataGenerator(ExportDataGenerator):
    serializer_class = ChannelListExportSerializer
    terms_filter = TERMS_FILTER
    ordering_fields = ORDERING_FIELDS
    queryset = ESQuerysetAdapter(ChannelManager((
        Sections.MAIN,
        Sections.GENERAL_DATA,
        Sections.STATS,
        Sections.ADS_STATS,
        Sections.BRAND_SAFETY,
    )))\
        .order_by(ORDERING_FIELDS[0])\
        .with_limit(100)


@celery_app.task
def export_channels_data(query_params, export_name, user_emails, export_url):
    content_exporter = ExportContextManager(
        HighlightsChannelListDataGenerator(query_params),
        CHANNEL_CSV_HEADERS
    )
    ESDataS3Exporter.export_to_s3(content_exporter, export_name)

    # prepare E-mail
    subject = "Export Channels"
    body = f"File is ready for downloading.\n" \
           f"Please, go to {export_url} to download the report.\n" \
           f"NOTE: url to download report is valid during next 2 weeks\n"

    # E-mail
    from_email = settings.EXPORTS_EMAIL_ADDRESS
    bcc = []
    try:
        email = EmailMessage(
            subject=subject,
            body=body,
            from_email=from_email,
            to=user_emails,
            bcc=bcc,
        )
        email.send(fail_silently=False)
    except Exception as e:
        logger.info(f"RESEARCH EXPORT: Error during sending email to {user_emails}: {e}")
    else:
        logger.info(f"RESEARCH EXPORT: Email was sent to {user_emails}.")

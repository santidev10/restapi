from saas import celery_app
from django.urls import reverse
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
from highlights.api.urls.names import HighlightsNames
from saas.urls.namespaces import Namespace



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
def export_channels_data(query_params, export_name, user_emails):
    content_exporter = ExportContextManager(
        HighlightsChannelListDataGenerator(query_params),
        CHANNEL_CSV_HEADERS
    )
    ESDataS3Exporter.export_to_s3(content_exporter, export_name)

    url_to_export = reverse(
        "{}:{}".format(Namespace.HIGHLIGHTS,  HighlightsNames.CHANNELS_EXPORT),
        args=(export_name,)
    )

    # prepare E-mail
    subject = "Export Channels"
    body = f"File is ready for downloading.\n" \
           f"Please, go to {settings.HOST + url_to_export} to download the report.\n" \
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
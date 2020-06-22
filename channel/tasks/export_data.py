import logging

from saas import celery_app
from django.conf import settings
from django.core.mail import send_mail

from es_components.constants import Sections
from es_components.managers import ChannelManager
from channel.constants import TERMS_FILTER
from channel.constants import MATCH_PHRASE_FILTER
from channel.constants import RANGE_FILTER
from channel.constants import EXISTS_FILTER
from channel.constants import CHANNEL_CSV_HEADERS
from channel.api.serializers.channel_export import ChannelListExportSerializer
from channel.utils import ChannelGroupParamAdapter
from channel.utils import IsTrackedParamsAdapter
from channel.utils import VettedParamsAdapter
from utils.es_components_api_utils import BrandSafetyParamAdapter
from utils.es_components_api_utils import ExportDataGenerator
from utils.es_components_api_utils import ESQuerysetAdapter
from utils.es_components_exporter import ESDataS3Exporter
from utils.aws.export_context_manager import ExportContextManager

logger = logging.getLogger(__name__)


class ChannelListDataGenerator(ExportDataGenerator):
    serializer_class = ChannelListExportSerializer
    terms_filter = TERMS_FILTER
    range_filter = RANGE_FILTER
    match_phrase_filter = MATCH_PHRASE_FILTER
    exists_filter = EXISTS_FILTER
    params_adapters = (BrandSafetyParamAdapter, ChannelGroupParamAdapter, VettedParamsAdapter, IsTrackedParamsAdapter)
    queryset = ESQuerysetAdapter(ChannelManager((
        Sections.MAIN,
        Sections.GENERAL_DATA,
        Sections.STATS,
        Sections.ADS_STATS,
        Sections.BRAND_SAFETY,
    )))


@celery_app.task
def export_channels_data(query_params, export_name, user_emails):
    content_exporter = ExportContextManager(
        ChannelListDataGenerator(query_params),
        CHANNEL_CSV_HEADERS
    )
    ESDataS3Exporter.export_to_s3(content_exporter, export_name)

    export_url = ESDataS3Exporter.generate_temporary_url(ESDataS3Exporter.get_s3_key(export_name), time_limit=86400)

    # prepare E-mail
    subject = "Export Channels"
    body = f"Export is ready to download.\n" \
           f"Please click <a href='{export_url}'>here</a> to download the report.\n" \
           f"NOTE: Link will expire in 24 hours.\n"

    # E-mail
    from_email = settings.EXPORTS_EMAIL_ADDRESS
    bcc = []

    try:
        send_mail(subject=subject, message=None, from_email=from_email, recipient_list=user_emails, html_message=body)
    # pylint: disable=broad-except
    except Exception as e:
    # pylint: enable=broad-except
        logger.info(f"RESEARCH EXPORT: Error during sending email to {user_emails}: {e}")
    else:
        logger.info(f"RESEARCH EXPORT: Email was sent to {user_emails}.")

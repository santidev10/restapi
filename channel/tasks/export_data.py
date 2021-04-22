import logging

from django.conf import settings

from administration.notifications import send_email
from channel.api.serializers.channel_export import ChannelListExportSerializer
from channel.constants import CHANNEL_CSV_HEADERS
from channel.constants import EXISTS_FILTER
from channel.constants import MATCH_PHRASE_FILTER
from channel.constants import RANGE_FILTER
from channel.constants import TERMS_FILTER
from channel.constants import MUST_NOT_TERMS_FILTER
from channel.utils import ChannelGroupParamAdapter
from channel.utils import IsTrackedParamsAdapter
from channel.utils import VettedParamsAdapter
from es_components.constants import Sections
from es_components.managers import ChannelManager
from saas import celery_app
from utils.aws.export_context_manager import ExportContextManager
from utils.es_components_api_utils import BrandSafetyParamAdapter
from utils.es_components_api_utils import ESQuerysetAdapter
from utils.es_components_api_utils import ExportDataGenerator
from utils.es_components_exporter import ESDataS3Exporter

logger = logging.getLogger(__name__)


class ChannelListDataGenerator(ExportDataGenerator):
    serializer_class = ChannelListExportSerializer
    terms_filter = TERMS_FILTER
    must_not_terms_filter = MUST_NOT_TERMS_FILTER
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
    send_email(subject=subject, message=None, from_email=from_email, recipient_list=user_emails, html_message=body)
    logger.info("RESEARCH EXPORT: Email was sent to %s.", user_emails)

import logging

from django.conf import settings
from django.core.mail import EmailMessage

from es_components.constants import Sections
from es_components.managers import KeywordManager
from keywords.api.serializers.keyword_export import KeywordListExportSerializer
from keywords.constants import KEYWORD_CSV_HEADERS
from keywords.constants import RANGE_FILTER
from keywords.constants import TERMS_FILTER
from keywords.utils import KeywordViralParamAdapter
from saas import celery_app
from utils.aws.export_context_manager import ExportContextManager
from utils.es_components_api_utils import ESQuerysetAdapter
from utils.es_components_api_utils import ExportDataGenerator
from utils.es_components_exporter import ESDataS3Exporter

logger = logging.getLogger(__name__)


class KeywordListDataGenerator(ExportDataGenerator):
    serializer_class = KeywordListExportSerializer
    terms_filter = TERMS_FILTER
    range_filter = RANGE_FILTER
    params_adapters = (KeywordViralParamAdapter,)
    queryset = ESQuerysetAdapter(KeywordManager((
        Sections.MAIN,
        Sections.STATS,
    )))


@celery_app.task
def export_keywords_data(query_params, export_name, user_emails):
    content_exporter = ExportContextManager(
        KeywordListDataGenerator(query_params),
        KEYWORD_CSV_HEADERS
    )
    ESDataS3Exporter.export_to_s3(content_exporter, export_name)

    export_url = ESDataS3Exporter.generate_temporary_url(ESDataS3Exporter.get_s3_key(export_name), time_limit=86400)

    # prepare E-mail
    subject = "Export Keywords"
    body = f"File is ready for downloading.\n" \
           f"Please, go to {export_url} to download the report.\n" \
           f"NOTE: url to download report is valid during next 24 hours\n"

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
    # pylint: disable=broad-except
    except Exception as e:
        # pylint: enable=broad-except
        logger.info("RESEARCH EXPORT: Error during sending email to %s: %s", user_emails, e)
    else:
        logger.info("RESEARCH EXPORT: Email was sent to %s.", user_emails)

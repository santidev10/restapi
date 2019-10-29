import logging

from saas import celery_app
from django.conf import settings
from django.core.mail import EmailMessage

from es_components.constants import Sections
from es_components.managers import KeywordManager
from utils.es_components_api_utils import ExportDataGenerator
from utils.es_components_api_utils import ESQuerysetAdapter
from utils.es_components_exporter import ESDataS3Exporter
from utils.aws.export_context_manager import ExportContextManager

from highlights.api.views.keywords import ORDERING_FIELDS
from highlights.api.views.keywords import TERMS_FILTER
from keywords.constants import KEYWORD_CSV_HEADERS
from keywords.api.serializers.keyword_export import KeywordListExportSerializer

logger = logging.getLogger(__name__)


class HighlightsKeywordListDataGenerator(ExportDataGenerator):
    serializer_class = KeywordListExportSerializer
    terms_filter = TERMS_FILTER
    ordering_fields = ORDERING_FIELDS
    queryset = ESQuerysetAdapter(KeywordManager((
        Sections.STATS,
    )))\
        .order_by(ORDERING_FIELDS[0])\
        .with_limit(100)


@celery_app.task
def export_keywords_data(query_params, export_name, user_emails, export_url):
    content_exporter = ExportContextManager(
        HighlightsKeywordListDataGenerator(query_params),
        KEYWORD_CSV_HEADERS
    )
    ESDataS3Exporter.export_to_s3(content_exporter, export_name)

    # prepare E-mail
    subject = "Export Keywords"
    body = f"File is ready for downloading.\n" \
           f"Please, go to {export_url} to download the report.\n" \
           f"NOTE: url to download report is valid during next 2 weeks\n"

    # E-mail
    from_email = settings.SENDER_EMAIL_ADDRESS
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

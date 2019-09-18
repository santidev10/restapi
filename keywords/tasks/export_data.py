from saas import celery_app
from django.urls import reverse
from django.conf import settings
from django.core.mail import EmailMessage

from es_components.constants import Sections
from es_components.managers import KeywordManager
from utils.es_components_api_utils import ExportDataGenerator
from utils.es_components_api_utils import ESQuerysetAdapter
from utils.es_components_exporter import ESDataS3Exporter
from utils.aws.export_context_manager import ExportContextManager

from keywords.constants import TERMS_FILTER
from keywords.constants import RANGE_FILTER
from keywords.constants import MATCH_PHRASE_FILTER
from keywords.constants import KEYWORD_CSV_HEADERS
from keywords.api.names import KeywordPathName
from keywords.api.serializers.keyword_export import KeywordListExportSerializer

from saas.urls.namespaces import Namespace



class KeywordListDataGenerator(ExportDataGenerator):
    serializer_class = KeywordListExportSerializer
    terms_filter = TERMS_FILTER
    range_filter = RANGE_FILTER
    match_phrase_filter = MATCH_PHRASE_FILTER
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

    url_to_export = reverse(
        "{}:{}".format(Namespace.KEYWORD,  KeywordPathName.KEYWORD_EXPORT),
        args=(export_name,)
    )

    # prepare E-mail
    subject = "Export Keywords"
    body = f"File is ready for downloading.\n" \
           f"Please, go to {url_to_export} to download the report.\n" \
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
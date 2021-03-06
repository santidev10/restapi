from django.conf import settings

from administration.notifications import send_email_with_headers
from aw_reporting.csv_reports import PacingReportCSVExport
from aw_reporting.reports.pacing_report import PacingReport
from saas import celery_app
from userprofile.models import UserProfile


@celery_app.task
def export_pacing_report(get, user_pk, report_name, url_to_export, user_emails):
    user = UserProfile.objects.get(pk=user_pk)

    pacing_report = PacingReport()
    opportunities = pacing_report.get_opportunities(get, user)

    csv_generator = PacingReportCSVExport(pacing_report, opportunities, report_name)
    csv_generator.export_to_s3()

    # prepare E-mail
    subject = "Pacing Report"
    body = "Pacing Report is ready for downloading.\n" \
           "Please, go to {url} to download the report.\n" \
           "NOTE: url to download report is valid during next 2 weeks\n".format(url=url_to_export)

    # E-mail
    from_email = settings.EXPORTS_EMAIL_ADDRESS
    send_email_with_headers(subject=subject, body=body, from_email=from_email, to=user_emails)

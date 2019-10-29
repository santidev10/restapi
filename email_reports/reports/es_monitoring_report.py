from django.conf import settings
from django.core.mail import EmailMultiAlternatives
from django.template.loader import get_template
from django.template.defaultfilters import striptags
from django.core.mail import EmailMessage

from email_reports.reports.base import BaseEmailReport
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.managers import KeywordManager
from es_components.constants import Sections
from utils.datetime import now_in_default_tz


class ESMonitoringEmailReport(BaseEmailReport):

    def __init__(self, *args, **kwargs):
        self.monitoring_reports = {}
        self.cluster = None
        self.today = now_in_default_tz().date()

        super(ESMonitoringEmailReport, self).__init__(*args, **kwargs)

    def send(self):
        self._collect_report()
        self.send_alerts()

        html_content = self._get_body()
        text_content = striptags(html_content)

        msg = EmailMultiAlternatives(
            self._get_subject(),
            text_content,
            from_email=settings.SENDER_EMAIL_ADDRESS,
            to=settings.ES_MONITORING_EMAIL_ADDRESSES,
            headers={"X-Priority": 2},
            reply_to="",
        )
        msg.attach_alternative(html_content, "text/html")
        msg.send(fail_silently=False)


    def _send_alert_email(self, model_name, alert_message):
        subject = f"DMP ALERT: {self.cluster} [{self.today}]"
        body = f"{model_name}: {alert_message}"
        email = EmailMessage(
            subject=subject,
            body=body,
            from_email=settings.EMERGENCY_SENDER_EMAIL_ADDRESS,
            to=settings.ES_MONITORING_EMAIL_ADDRESSES,
            bcc=[],
        )
        email.send(fail_silently=False)


    def send_alerts(self):
        for model_name, report in self.monitoring_reports.items():
            alerts = report.get("alerts")

            if not alerts:
                continue

            for alert in alerts:
                self._send_alert_email(model_name, alert)


    def _collect_report(self):
        managers = [
            ChannelManager([Sections.GENERAL_DATA, Sections.STATS, Sections.ADS_STATS, Sections.ANALYTICS,
                            Sections.AUTH, Sections.CMS]),
            VideoManager([Sections.GENERAL_DATA, Sections.STATS, Sections.ADS_STATS,
                          Sections.ANALYTICS, Sections.CAPTIONS, Sections.CMS]),
            KeywordManager(Sections.STATS)
        ]

        for manager in managers:
            report = manager.get_monitoring_data()

            if not self.cluster:
                self.cluster = report.get("cluster_name")

            self.monitoring_reports[manager.model.__name__] = report

    def _get_body(self):
        html = get_template("es_monitoring_data_report.html")
        html_content = html.render({"reports": self.monitoring_reports})
        return html_content

    def _get_subject(self):
        return f"ElasticSearch data monitoring report ({self.cluster}) [{self.today}]"

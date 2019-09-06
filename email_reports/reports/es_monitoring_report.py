from datetime import timedelta

from django.conf import settings
from django.core.mail import EmailMultiAlternatives

from email_reports.reports.base import BaseEmailReport
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.managers import KeywordManager
from es_components.constants import Sections


class ESMonitoringEmailReport(BaseEmailReport):

    def send(self):
        msg = EmailMultiAlternatives(
            "ES Monitoring Report",
            self._build_body(),
            from_email=settings.SENDER_EMAIL_ADDRESS,
            to=settings.ES_MONITORING_EMAIL_ADDRESSES,
            headers={'X-Priority': 2},
            reply_to="",
        )
        msg.send(fail_silently=False)

    def _build_body(self):
        managers = [
            ChannelManager([Sections.GENERAL_DATA, Sections.STATS, Sections.ADS_STATS, Sections.ANALYTICS]),
            VideoManager([Sections.GENERAL_DATA, Sections.STATS, Sections.ADS_STATS,
                          Sections.ANALYTICS, Sections.CAPTIONS]),
            KeywordManager(Sections.STATS)
        ]
        cluster = None
        text_content = ""

        for manager in managers:
            _cluster, report = manager.get_monitoring_info()

            if not cluster:
                cluster = _cluster

            text_content += self._get_text_content(report, manager.model._index._name)

        text_content = f"ElasticSearch Monitoring Report for {cluster}\n{text_content}"

        return text_content


    def _get_text_content(self, report, es_model_name):
        filled_missed_data = ""
        created_by_days = ""
        updated_by_days = ""
        totals = report.get("index").get("docs.count")

        def text_by_days(last_day, last_3_days, last_7_days, last_30_days, last_365_days):
            return f"{last_day[0]} ({last_day[1]}%) - last day, " \
                   f"{last_3_days[0]} ({last_3_days[1]}%) - last 3 days, \n" \
                   f"{last_7_days[0]} ({last_7_days[1]}%) - last 7 days, " \
                   f"{last_30_days[0]} ({last_30_days[1]}%) - last 30 days,  " \
                   f"{last_365_days[0]} ({last_365_days[1]}%) - last 365 days \n"


        for section, data in report.get("performance").items():
            filled_missed_data += f"{section}:  {data.filled[0]} ({data.filled[1]})% filled, " \
                                  f" {data.missed[0]} ({data.missed[1]})% missed\n"


            created_by_days += f"{section}: {text_by_days(*data.created_by_days)}\n"
            updated_by_days += f"{section}: {text_by_days(*data.updated_by_days)}\n"

        result_text = f"{es_model_name} - totals {totals}\n" \
                      f"{filled_missed_data}\n" \
                      f"Created sections by days\n" \
                      f"{created_by_days}" \
                      f"Updated sections by days\n" \
                      f"{updated_by_days}"

        return result_text

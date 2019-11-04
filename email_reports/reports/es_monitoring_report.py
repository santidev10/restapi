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
        #
        # for manager in managers:
        #
        #     report = manager.get_monitoring_data()
        #
        #     if not self.cluster:
        #         self.cluster = report.get("cluster_name")
        #
        #     self.monitoring_reports[manager.model.__name__] = report

        self.monitoring_reports = {'Channel': {'cluster_name': 'chf-elastic-rc',
                     'warnings': ['Less than 90% of analytics data has been updated during the last day',
                                  'No new analytics,auth,cms sections in the last 3 days'], 'info': {
                'index': {'index': 'channels_20191008', 'docs_count': '2584930', 'docs_deleted': '708069',
                          'store_size': '23.4gb', 'pri_store_size': '8.4gb', 'health': 'green', 'pri': '24',
                          'rep': '1'}, 'performance': {
                    'general_data': {'filled': 801807, 'missed': 8950, 'deleted': 1774173,
                                     'missed_by_days': {'last_day': 217, 'last_3_days': 573, 'last_7_days': 663,
                                                        'last_30_days': 2255, 'last_365_days': 11110},
                                     'updated_by_days': {'last_day': 469090, 'last_3_days': 790236,
                                                         'last_7_days': 790659, 'last_30_days': 799956,
                                                         'last_365_days': 2573820},
                                     'created_by_days': {'last_day': 20485, 'last_3_days': 64307, 'last_7_days': 149217,
                                                         'last_30_days': 657923, 'last_365_days': 2573820}},
                    'stats': {'filled': 799214, 'missed': 11543, 'deleted': 1774173,
                              'missed_by_days': {'last_day': 297, 'last_3_days': 653, 'last_7_days': 1741,
                                                 'last_30_days': 4781, 'last_365_days': 11543},
                              'updated_by_days': {'last_day': 389906, 'last_3_days': 636615, 'last_7_days': 720525,
                                                  'last_30_days': 796991, 'last_365_days': 2573387},
                              'created_by_days': {'last_day': 19946, 'last_3_days': 63756, 'last_7_days': 147666,
                                                  'last_30_days': 656802, 'last_365_days': 2573387}},
                    'ads_stats': {'filled': 15388, 'missed': 795369, 'deleted': 1774173,
                                  'missed_by_days': {'last_day': 22759, 'last_3_days': 174322, 'last_7_days': 258941,
                                                     'last_30_days': 720506, 'last_365_days': 2568953},
                                  'updated_by_days': {'last_day': 0, 'last_3_days': 15963, 'last_7_days': 15964,
                                                      'last_30_days': 15968, 'last_365_days': 15977},
                                  'created_by_days': {'last_day': 0, 'last_3_days': 82, 'last_7_days': 655,
                                                      'last_30_days': 4187, 'last_365_days': 15977}},
                    'analytics': {'filled': 3218, 'missed': 807539, 'deleted': 1774173,
                                  'missed_by_days': {'last_day': 24969, 'last_3_days': 184867, 'last_7_days': 269825,
                                                     'last_30_days': 733493, 'last_365_days': 2581712},
                                  'updated_by_days': {'last_day': 2898, 'last_3_days': 2933, 'last_7_days': 2933,
                                                      'last_30_days': 2937, 'last_365_days': 3218},
                                  'created_by_days': {'last_day': 0, 'last_3_days': 0, 'last_7_days': 0,
                                                      'last_30_days': 276, 'last_365_days': 3218}},
                    'auth': {'filled': 5221, 'missed': 805536, 'deleted': 1774173,
                             'missed_by_days': {'last_day': 24839, 'last_3_days': 182810, 'last_7_days': 267768,
                                                'last_30_days': 731417, 'last_365_days': 2579709},
                             'updated_by_days': {'last_day': 0, 'last_3_days': 0, 'last_7_days': 0, 'last_30_days': 1,
                                                 'last_365_days': 5221},
                             'created_by_days': {'last_day': 0, 'last_3_days': 0, 'last_7_days': 0, 'last_30_days': 1,
                                                 'last_365_days': 5168}},
                    'cms': {'filled': 6582, 'missed': 804175, 'deleted': 1774173,
                            'missed_by_days': {'last_day': 24831, 'last_3_days': 181887, 'last_7_days': 266845,
                                               'last_30_days': 730492, 'last_365_days': 2578348},
                            'updated_by_days': {'last_day': 0, 'last_3_days': 0, 'last_7_days': 0, 'last_30_days': 0,
                                                'last_365_days': 6582},
                            'created_by_days': {'last_day': 0, 'last_3_days': 0, 'last_7_days': 0, 'last_30_days': 0,
                                                'last_365_days': 6582}},
                    'main': {'filled': 810757, 'missed': 0, 'deleted': 1774173,
                             'missed_by_days': {'last_day': 0, 'last_3_days': 0, 'last_7_days': 0, 'last_30_days': 0,
                                                'last_365_days': 0},
                             'updated_by_days': {'last_day': 690626, 'last_3_days': 815116, 'last_7_days': 815118,
                                                 'last_30_days': 821732, 'last_365_days': 2584930},
                             'created_by_days': {'last_day': 25075, 'last_3_days': 187239, 'last_7_days': 272197,
                                                 'last_30_days': 735874, 'last_365_days': 2584930}}}}, 'alerts': []},
         'Video': {'cluster_name': 'chf-elastic-rc', 'warnings': ['No new captions,cms sections in the last 3 days'],
                   'info': {'index': {'index': 'videos_20191008', 'docs_count': '9009434', 'docs_deleted': '2202748',
                                      'store_size': '133.2gb', 'pri_store_size': '59.1gb', 'health': 'green',
                                      'pri': '24', 'rep': '1'}, 'performance': {
                       'general_data': {'filled': 8662033, 'missed': 150032, 'deleted': 197369,
                                        'missed_by_days': {'last_day': 2292, 'last_3_days': 3446, 'last_7_days': 3586,
                                                           'last_30_days': 4939, 'last_365_days': 159335},
                                        'updated_by_days': {'last_day': 2767440, 'last_3_days': 8278233,
                                                            'last_7_days': 8278233, 'last_30_days': 8397796,
                                                            'last_365_days': 8850099},
                                        'created_by_days': {'last_day': 571, 'last_3_days': 12695, 'last_7_days': 12696,
                                                            'last_30_days': 119589, 'last_365_days': 8850099}},
                       'stats': {'filled': 8671811, 'missed': 140254, 'deleted': 197369,
                                 'missed_by_days': {'last_day': 1632, 'last_3_days': 2038, 'last_7_days': 2170,
                                                    'last_30_days': 4543, 'last_365_days': 148137},
                                 'updated_by_days': {'last_day': 4336276, 'last_3_days': 8215078,
                                                     'last_7_days': 8215117, 'last_30_days': 8378593,
                                                     'last_365_days': 8861297},
                                 'created_by_days': {'last_day': 11929, 'last_3_days': 12927, 'last_7_days': 12928,
                                                     'last_30_days': 121422, 'last_365_days': 8861297}},
                       'ads_stats': {'filled': 2930, 'missed': 8809135, 'deleted': 197369,
                                     'missed_by_days': {'last_day': 89726, 'last_3_days': 1486757,
                                                        'last_7_days': 1498268, 'last_30_days': 2399181,
                                                        'last_365_days': 9006491},
                                     'updated_by_days': {'last_day': 0, 'last_3_days': 2933, 'last_7_days': 2933,
                                                         'last_30_days': 2934, 'last_365_days': 2943},
                                     'created_by_days': {'last_day': 0, 'last_3_days': 1, 'last_7_days': 3,
                                                         'last_30_days': 77, 'last_365_days': 2943}},
                       'analytics': {'filled': 125002, 'missed': 8687063, 'deleted': 197369,
                                     'missed_by_days': {'last_day': 87188, 'last_3_days': 1465540,
                                                        'last_7_days': 1476882, 'last_30_days': 2371262,
                                                        'last_365_days': 8884432},
                                     'updated_by_days': {'last_day': 59896, 'last_3_days': 62336, 'last_7_days': 63307,
                                                         'last_30_days': 66422, 'last_365_days': 125002},
                                     'created_by_days': {'last_day': 269, 'last_3_days': 454, 'last_7_days': 849,
                                                         'last_30_days': 4825, 'last_365_days': 125002}},
                       'captions': {'filled': 394832, 'missed': 8417233, 'deleted': 197369,
                                    'missed_by_days': {'last_day': 80381, 'last_3_days': 1416208,
                                                       'last_7_days': 1427162, 'last_30_days': 2325406,
                                                       'last_365_days': 8614589},
                                    'updated_by_days': {'last_day': 0, 'last_3_days': 0, 'last_7_days': 0,
                                                        'last_30_days': 0, 'last_365_days': 394845},
                                    'created_by_days': {'last_day': 0, 'last_3_days': 0, 'last_7_days': 0,
                                                        'last_30_days': 0, 'last_365_days': 394845}},
                       'cms': {'filled': 829929, 'missed': 7982136, 'deleted': 197369,
                               'missed_by_days': {'last_day': 76472, 'last_3_days': 1375222, 'last_7_days': 1386176,
                                                  'last_30_days': 2277169, 'last_365_days': 8179492},
                               'updated_by_days': {'last_day': 0, 'last_3_days': 0, 'last_7_days': 0, 'last_30_days': 0,
                                                   'last_365_days': 829942},
                               'created_by_days': {'last_day': 0, 'last_3_days': 0, 'last_7_days': 0, 'last_30_days': 0,
                                                   'last_365_days': 829942}},
                       'main': {'filled': 8812065, 'missed': 0, 'deleted': 197369,
                                'missed_by_days': {'last_day': 0, 'last_3_days': 0, 'last_7_days': 0, 'last_30_days': 0,
                                                   'last_365_days': 0},
                                'updated_by_days': {'last_day': 5626832, 'last_3_days': 8638058, 'last_7_days': 8649382,
                                                    'last_30_days': 8733571, 'last_365_days': 9009434},
                                'created_by_days': {'last_day': 89847, 'last_3_days': 1489403, 'last_7_days': 1500921,
                                                    'last_30_days': 2401860, 'last_365_days': 9009434}}}},
                   'alerts': []}, 'Keyword': {'cluster_name': 'chf-elastic-rc', 'warnings': [], 'info': {
            'index': {'index': 'keywords_20191008', 'docs_count': '5379291', 'docs_deleted': '1392572',
                      'store_size': '14.7gb', 'pri_store_size': '5gb', 'health': 'green', 'pri': '24', 'rep': '1'},
            'performance': {'stats': {'filled': 5379291, 'missed': 0, 'deleted': None,
                                      'missed_by_days': {'last_day': 0, 'last_3_days': 0, 'last_7_days': 0,
                                                         'last_30_days': 0, 'last_365_days': 0},
                                      'updated_by_days': {'last_day': 704861, 'last_3_days': 2262020,
                                                          'last_7_days': 5377741, 'last_30_days': 5379291,
                                                          'last_365_days': 5379291},
                                      'created_by_days': {'last_day': 0, 'last_3_days': 0, 'last_7_days': 0,
                                                          'last_30_days': 0, 'last_365_days': 5379291}},
                            'main': {'filled': 5379291, 'missed': 0, 'deleted': None,
                                     'missed_by_days': {'last_day': 0, 'last_3_days': 0, 'last_7_days': 0,
                                                        'last_30_days': 0, 'last_365_days': 0},
                                     'updated_by_days': {'last_day': 749920, 'last_3_days': 2308530,
                                                         'last_7_days': 5379291, 'last_30_days': 5379291,
                                                         'last_365_days': 5379291},
                                     'created_by_days': {'last_day': 0, 'last_3_days': 0, 'last_7_days': 0,
                                                         'last_30_days': 0, 'last_365_days': 5379291}}}}, 'alerts': []}}

    def _get_body(self):
        html = get_template("es_monitoring_data_report.html")
        html_content = html.render({"reports": self.monitoring_reports})
        return html_content

    def _get_subject(self):
        return f"ElasticSearch data monitoring report ({self.cluster}) [{self.today}]"

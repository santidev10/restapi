from django.apps import AppConfig


class AdsAnalyzerConfig(AppConfig):
    name = "ads_analyzer"

    def ready(self):
        super().ready()
        # pylint: disable=unused-import
        from ads_analyzer.reports.opportunity_targeting_report.create_report import save_opportunity_report_receiver
        # pylint: disable=unused-import

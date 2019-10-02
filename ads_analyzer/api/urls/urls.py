from django.conf.urls import url

from ads_analyzer.api.urls.names import AdsAnalyzerPathName
from ads_analyzer.api.views.opportunity_list import OpportunityListAPIView
from ads_analyzer.api.views.opportunity_targeting_report import OpportunityTargetingReportAPIView

urlpatterns = (
    url(
        r"^opportunity_list",
        OpportunityListAPIView.as_view(),
        name=AdsAnalyzerPathName.OPPORTUNITY_LIST
    ),
    url(
        r"^opportunity_targeting_report",
        OpportunityTargetingReportAPIView.as_view(),
        name=AdsAnalyzerPathName.OPPORTUNITY_TARGETING_REPORT
    ),
)

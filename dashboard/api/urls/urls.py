"""
Audit Tool api urls module
"""
from django.conf.urls import url

from .names import DashboardPathName
from dashboard.api.views import DashboardListAPIView
from dashboard.api.views import DashboardPacingAlertsAPIView
from dashboard.api.views import DashboardAuditQueueAPIView
from dashboard.api.views import DashboardIndustryPerformanceAPIView

urlpatterns = [
    url(r"^dashboard/$", DashboardListAPIView.as_view(), name=DashboardPathName.DASHBOARD_LIST),
    url(r"^dashboard/pacing_alerts/$", DashboardPacingAlertsAPIView.as_view(),
        name=DashboardPathName.DASHBOARD_PACING_ALERTS),
    url(r"^dashboard/audit_queue/$", DashboardAuditQueueAPIView.as_view(),
        name=DashboardPathName.DASHBOARD_AUDIT_QUEUE),
    url(r"^dashboard/industry_performance/$", DashboardIndustryPerformanceAPIView.as_view(),
        name=DashboardPathName.DASHBOARD_INDUSTRY_PERFORMANCE)
]

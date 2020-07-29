"""
Audit Tool api urls module
"""
from django.conf.urls import url

from .names import DashboardPathName
from dashboard.api.views import DashboardListAPIView
from dashboard.api.views import DashboardPacingAlertsAPIView

urlpatterns = [
    url(r"^dashboard/$", DashboardListAPIView.as_view(), name=DashboardPathName.DASHBOARD_LIST),
    url(r"^dashboard/pacing_alerts/$", DashboardPacingAlertsAPIView.as_view(),
        name=DashboardPathName.DASHBOARD_PACING_ALERTS),
]

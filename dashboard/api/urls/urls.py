"""
Audit Tool api urls module
"""
from django.conf.urls import url

from .names import DashboardPathName
from dashboard.api.views import DashboardListAPIView
from dashboard.api.views import DashboardPacingAlertsAPIView

urlpatterns = [
    url(r"^dashboard/(?P<pk>\w+)/$", DashboardListAPIView.as_view(), name=DashboardPathName.DASHBOARD_LIST),
    url(r"^dashboard/pacing_alerts/(?P<pk>\w+)/$", DashboardPacingAlertsAPIView.as_view(),
        name=DashboardPathName.DASHBOARD_PACING_ALERTS),
]

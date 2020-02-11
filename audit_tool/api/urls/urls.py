"""
Audit Tool api urls module
"""
from django.conf.urls import url

from audit_tool.api.views import AuditExportApiView
from audit_tool.api.views import AuditListApiView
from audit_tool.api.views import AuditResumeApiView
from audit_tool.api.views import AuditSaveApiView
from audit_tool.api.views import AuditStopApiView
from audit_tool.api.views import AuditFlagApiView
from audit_tool.api.views import AuditHistoryApiView
from audit_tool.api.views import AuditPauseApiView
from audit_tool.api.views import AuditVetRetrieveUpdateAPIView
from audit_tool.api.views import AuditVettingOptionsAPIView
from audit_tool.api.views import AuditAdminAPIView

from .names import AuditPathName

urlpatterns = [
    url(r"^audit_tool/audits/list/$", AuditListApiView.as_view(), name=AuditPathName.AUDIT_LIST),
    url(r"^audit_tool/audits/save/$", AuditSaveApiView.as_view(), name=AuditPathName.AUDIT_SAVE),
    url(r"^audit_tool/audits/export/$", AuditExportApiView.as_view(), name=AuditPathName.AUDIT_EXPORT),
    url(r"^audit_tool/audits/resume/$", AuditResumeApiView.as_view(), name=AuditPathName.AUDIT_RESUME),
    url(r"^audit_tool/audits/stop/$", AuditStopApiView.as_view(), name=AuditPathName.AUDIT_STOP),
    url(r"^audit_tool/audits/flag/$", AuditFlagApiView.as_view(), name=AuditPathName.AUDIT_FLAG),
    url(r"^audit_tool/audits/history/$", AuditHistoryApiView.as_view(), name=AuditPathName.AUDIT_HISTORY),
    url(r"^audit_tool/audits/pause/$", AuditPauseApiView.as_view(), name=AuditPathName.AUDIT_PAUSE),
    url(r"^audit_tool/audits/vet/(?P<pk>\d+)/$", AuditVetRetrieveUpdateAPIView.as_view(), name=AuditPathName.AUDIT_PAUSE),
    url(r"^audit_tool/audits/vetting_options/$", AuditVettingOptionsAPIView.as_view(), name=AuditPathName.AUDIT_VETTING_OPTIONS),
    url(r"^audit_tool/audits/admin/$", AuditAdminAPIView.as_view(), name=AuditPathName.AUDIT_ADMIN),
]

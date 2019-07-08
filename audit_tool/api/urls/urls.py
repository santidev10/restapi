"""
Audit Tool api urls module
"""
from django.conf.urls import url

from audit_tool.api.views import AuditExportApiView
from audit_tool.api.views import AuditListApiView
from audit_tool.api.views import AuditResumeApiView
from audit_tool.api.views import AuditSaveApiView
from audit_tool.api.views import AuditStopApiView

from .names import AuditPathName

urlpatterns = [
    url(r"^audit_tool/audits/list/$", AuditListApiView.as_view(), name=AuditPathName.AUDIT_LIST),
    url(r"^audit_tool/audits/save/$", AuditSaveApiView.as_view(), name=AuditPathName.AUDIT_SAVE),
    url(r"^audit_tool/audits/export/$", AuditExportApiView.as_view(), name=AuditPathName.AUDIT_EXPORT),
    url(r"^audit_tool/audits/resume/$", AuditResumeApiView.as_view(), name=AuditPathName.AUDIT_RESUME),
    url(r"^audit_tool/audits/stop/$", AuditStopApiView.as_view(), name=AuditPathName.AUDIT_STOP)
]

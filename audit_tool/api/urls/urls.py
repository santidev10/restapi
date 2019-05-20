"""
Audit Tool api urls module
"""
from django.conf.urls import url

from audit_tool.api.views import AuditListApiView
from audit_tool.api.views import AuditSaveApiView

from .names import AuditPathName

urlpatterns = [
    url(r"^audit_tool/audits/list/$", AuditListApiView.as_view(), name=AuditPathName.AUDIT_LIST),
    url(r"^audit_tool/audits/save/$", AuditSaveApiView.as_view(), name=AuditPathName.AUDIT_SAVE)
]

"""
Audit Tool api urls module
"""
from django.conf.urls import url

from audit_tool.api.views.audit_list import AuditListApiView

from .names import AuditPathName

urlpatterns = [
    url(r"^audit_tool/audits/list/$", AuditListApiView.as_view(), name=AuditPathName.AUDIT_LIST)
]

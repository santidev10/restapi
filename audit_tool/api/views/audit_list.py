from rest_framework.views import APIView
from rest_framework.response import Response

class AuditListApiView(APIView):
    def get(self, running=None, audit_type=None):
        all = AuditProcessor.objects.all()
        if audit_type:
            all = all.filter(audit_type=audit_type)
        if running:
            all = all.filter(completed__isnull=running)
        ret = []
        for a in all.order_by("id"):
            ret.append({
                'id': a.id,
                'pause': a.pause,
                'completed': a.completed,
                'cached_data': a.cached_data,
                'name': a.params.get('name'),
                'audit_type': a.audit_type
            })
        return ret
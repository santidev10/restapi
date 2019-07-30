from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.exceptions import ValidationError
from audit_tool.models import AuditProcessor
from audit_tool.models import AuditVideoProcessor

class AuditResumeApiView(APIView):
    def post(self, request):
        query_params = request.query_params
        audit_id = query_params["audit_id"] if "audit_id" in query_params else None
        max_recommended = query_params["max_recommended"] if "max_recommended" in query_params else 100000
        if audit_id:
            try:
                audit = AuditProcessor.objects.get(id=audit_id, completed__isnull=False, audit_type=0)
                params = audit.params
                related_audits = params.get('related_audits')
                if not related_audits:
                    related_audits = [audit.id]
                else:
                    related_audits = related_audits + [audit.id]
                params['related_audits'] = related_audits
                if not params['name'].startswith('Resumed: '):
                    params['name'] = 'Resumed: {}'.format(params['name'])
                new_audit = AuditProcessor.objects.create(
                    audit_type=0,
                    params=params,
                    max_recommended=max_recommended,
                )
                # GET SOURCE VIDEOS FROM OLD AUDIT THAT WERE NOT PROCESSED AND COPY THEM AS SOURCE FOR THIS
                videos = AuditVideoProcessor.objects.filter(audit_id=audit.id, processed__isnull=True)
                for v in videos[:1000]:
                    try:
                        AuditVideoProcessor.objects.create(
                            audit=new_audit,
                            video=v.video,
                        )
                    except Exception as e:
                        pass
            except Exception as e:
                raise ValidationError("invalid audit_id: please verify you are resuming a completed 'recommendation' audit.")

            return Response(new_audit.to_dict())
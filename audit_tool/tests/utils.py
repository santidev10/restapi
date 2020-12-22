from audit_tool.models import AuditAgeGroup
from audit_tool.models import AuditContentType
from audit_tool.models import AuditContentQuality
from audit_tool.models import AuditGender


def create_model_objs():
    [
        AuditContentType.objects.get_or_create(id=_id, defaults=dict(id=_id, content_type=val))
        for _id, val in dict(AuditContentType.ID_CHOICES).items()
    ]
    [
        AuditGender.objects.get_or_create(id=_id, defaults=dict(id=_id, gender=val))
        for _id, val in dict(AuditGender.ID_CHOICES).items()
    ]
    [
        AuditAgeGroup.objects.get_or_create(id=_id, defaults=dict(id=_id, age_group=val))
        for _id, val in dict(AuditAgeGroup.ID_CHOICES).items()
    ]
    [
        AuditContentQuality.objects.get_or_create(id=_id, defaults=dict(id=_id, quality=val))
        for _id, val in dict(AuditContentQuality.ID_CHOICES).items()
    ]

from audit_tool.models import AuditAgeGroup
from audit_tool.models import AuditGender
from audit_tool.models import AuditContentQuality
from audit_tool.models import AuditContentType


def create_test_audit_objects():
    audit_age_groups = [
        AuditAgeGroup(id=id, age_group=age_group)
        for id, age_group in AuditAgeGroup.ID_CHOICES
    ]
    audit_genders = [
        AuditGender(id=id, gender=gender)
        for id, gender in AuditGender.ID_CHOICES
    ]
    audit_content_qualities = [
        AuditContentQuality(id=id, quality=quality)
        for id, quality in AuditContentQuality.ID_CHOICES
    ]
    audit_content_types = [
        AuditContentType(id=id, content_type=content_type)
        for id, content_type in AuditContentType.ID_CHOICES
    ]
    AuditAgeGroup.objects.bulk_create(audit_age_groups)
    AuditGender.objects.bulk_create(audit_genders)
    AuditContentQuality.objects.bulk_create(audit_content_qualities)
    AuditContentType.objects.bulk_create(audit_content_types)


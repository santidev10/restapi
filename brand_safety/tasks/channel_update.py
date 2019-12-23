from audit_tool.models import APIScriptTracker
from brand_safety.auditors.brand_safety_audit import BrandSafetyAudit
from brand_safety.constants import CHANNEL_DISCOVERY_TRACKER
from brand_safety.tasks.audit_manager import AuditManager

def channel_discovery():
    cursor = APIScriptTracker.objects.get_or_create(
        name=CHANNEL_DISCOVERY_TRACKER,
        defaults={
            "cursor_id": None
        }
    )
    manager = AuditManager(audit_type=1)

    auditor = BrandSafetyAudit()

    # task to batch channel ids to auditors
    # task batches of ~100 channels
from django.core.management.base import BaseCommand
from brand_safety.standard_brand_safety_provider import StandardBrandSafetyProvider
from audit_tool.models import APIScriptTracker
import logging

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Retrieve and audit comments.'

    def handle(self, *args, **kwargs):
        api_tracker = APIScriptTracker.objects.get(name="StandardAudit")
        standard_audit = StandardBrandSafetyProvider(api_tracker=api_tracker)
        standard_audit.run()

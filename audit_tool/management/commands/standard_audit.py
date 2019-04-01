from django.core.management.base import BaseCommand
from audit_tool.standard_audit import StandardAudit
from audit_tool.models import APIScriptTracker
from singledb.connector import SingleDatabaseApiConnector as SDBConnector
import logging

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Retrieve and audit comments.'

    def handle(self, *args, **kwargs):
        api_tracker = APIScriptTracker.objects.get(name="StandardAudit")
        standard_audit = StandardAudit(SDBConnector, api_tracker)
        standard_audit.run()

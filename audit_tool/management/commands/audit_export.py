import logging

from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils import timezone
from pid import PidFile
from time import sleep

from administration.notifications import send_email
from audit_tool.api.views.audit_export import AuditExportApiView
from audit_tool.api.views.audit_export import AuditS3Exporter
from audit_tool.models import AuditChannelProcessor
from audit_tool.models import AuditExporter
from audit_tool.models import AuditVideoProcessor

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument('thread_id', type=int)

    def handle(self, *args, **options):
        self.thread_id = options.get('thread_id')
        if not self.thread_id:
            self.thread_id = 0
        with PidFile(piddir='.', pidname='export_queue_{}.pid'.format(self.thread_id)) as p:
            try:
                self.machine_number = settings.AUDIT_MACHINE_NUMBER
            except Exception as e:
                self.machine_number = 0
            sleep(2 * (self.machine_number + self.thread_id))
            zombie_exports = AuditExporter.objects.filter(
                started__isnull=False,
                completed__isnull=True,
                machine=self.machine_number,
                thread=self.thread_id
            )
            if zombie_exports.count() > 0:
                zombie_exports.update(
                    started=None,
                    machine=None,
                    thread=None,
                )
            try:
                self.export = AuditExporter.objects.filter(completed__isnull=True, started__isnull=True).order_by("id")[0]
                self.audit = self.export.audit
            except Exception as e:
                logger.exception(e)
                raise Exception("no audits to export at present")
            self.process_export()

    def process_export(self):
        self.export.started = timezone.now()
        self.export.machine = self.machine_number
        self.export.thread = self.thread_id
        self.export.save(update_fields=['started', 'machine', 'thread'])
        export_funcs = AuditExportApiView()
        audit_type = self.audit.params.get('audit_type_original')
        if not audit_type:
            audit_type = self.audit.audit_type
        if audit_type == 2 and not self.export.export_as_videos:
            try:
                file_name, _ = export_funcs.export_channels(self.audit, self.audit.id, clean=self.export.clean, export=self.export)
            except Exception as e:
                self.export.started = None
                self.export.percent_done = 0
                self.export.save(update_fields=['started', 'percent_done'])
                raise Exception(e)
            count = AuditChannelProcessor.objects.filter(audit=self.audit)
        else:
            try:
                file_name, _ = export_funcs.export_videos(self.audit, self.audit.id, clean=self.export.clean, export=self.export)
            except Exception as e:
                self.export.started = None
                self.export.percent_done = 0
                self.export.save(update_fields=['started', 'percent_done'])
                raise Exception(e)
            count = AuditVideoProcessor.objects.filter(audit=self.audit)
        if self.export.clean is not None:
            count = count.filter(clean=self.export.clean)
        count = count.count()
        self.send_audit_email(file_name, settings.AUDIT_TOOL_EMAIL_RECIPIENTS, count)
        self.export.completed = timezone.now()
        self.export.file_name = file_name
        if self.audit.completed:
            self.export.final = True
        self.export.save(update_fields=['completed', 'file_name', 'final'])
        raise Exception("Audit {} exported with filename {}".format(self.audit.id, file_name))

    def send_audit_email(self, file_name, recipients, count):
        if count == 0:
            return
        file_url = AuditS3Exporter.generate_temporary_url(file_name, 604800)
        subject = "Audit '{}' Completed".format(self.audit.params['name'])
        body = "Audit '{}' has finished with {} results. Click " \
                   .format(self.audit.params['name'], "{:,}".format(count)) \
               + "<a href='{}'>here</a> to download. Link will expire in 7 days." \
                   .format(file_url)
        export_owner = self.export.owner
        if export_owner:
            recipients = [export_owner.email]
        send_email(
            subject=subject,
            from_email=settings.EXPORTS_EMAIL_ADDRESS,
            recipient_list=recipients,
            html_message=body,
        )

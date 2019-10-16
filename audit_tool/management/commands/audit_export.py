from django.core.mail import send_mail
from django.core.management.base import BaseCommand
import logging
logger = logging.getLogger(__name__)
from pid import PidFile
from audit_tool.models import AuditChannelProcessor
from audit_tool.models import AuditExporter
from audit_tool.models import AuditVideoProcessor
from audit_tool.api.views.audit_export import AuditExportApiView
from audit_tool.api.views.audit_export import AuditS3Exporter
from django.conf import settings
from django.utils import timezone

class Command(BaseCommand):
    def handle(self, *args, **options):
        with PidFile(piddir='.', pidname='export_queue.pid') as p:
            try:
                self.export = AuditExporter.objects.filter(completed__isnull=True).order_by("id")[0]
                self.audit = self.export.audit
            except Exception as e:
                logger.exception(e)
                raise Exception("no audits to export at present")
            self.process_export()

    def process_export(self):
        export_funcs = AuditExportApiView()
        audit_type = self.audit.params.get('audit_type_original')
        if not audit_type:
            audit_type = self.audit.audit_type
        if audit_type == 2:
            file_name, _ = export_funcs.export_channels(self.audit, self.audit.id, clean=self.export.clean)
            count = AuditChannelProcessor.objects.filter(audit=self.audit)
        else:
            file_name, _ = export_funcs.export_videos(self.audit, self.audit.id, clean=self.export.clean)
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
        send_mail(
            subject=subject,
            message=None,
            from_email=settings.SENDER_EMAIL_ADDRESS,
            recipient_list=recipients,
            html_message=body,
        )

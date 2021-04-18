import logging
from time import sleep

from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils import timezone
from pid import PidFile

from administration.notifications import send_email
from audit_tool.api.views.audit_export import AuditExportApiView
from audit_tool.api.views.audit_export import AuditS3Exporter
from audit_tool.models import AuditChannelProcessor
from audit_tool.models import AuditExporter
from audit_tool.models import AuditVideoProcessor

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def __init__(self, stdout=None, stderr=None, no_color=False, force_color=False):
        super(Command, self).__init__(stdout=stdout, stderr=stderr, no_color=no_color, force_color=force_color)
        self.thread_id = None
        self.machine_number = None
        self.export = None
        self.audit = None

    def add_arguments(self, parser):
        parser.add_argument("thread_id", type=int)

    def handle(self, *args, **options):
        self.thread_id = options.get("thread_id")
        if not self.thread_id:
            self.thread_id = 0
        with PidFile(piddir="pids", pidname="export_queue_{}.pid".format(self.thread_id)):
            try:
                self.machine_number = settings.AUDIT_MACHINE_NUMBER
            # pylint: disable=broad-except
            except Exception:
            # pylint: enable=broad-except
                self.machine_number = 0
            sleep(2 * (self.machine_number + self.thread_id))
            if self.machine_number is not None and self.thread_id is not None:
                zombie_exports = AuditExporter.objects.filter(
                    started__isnull=False,
                    completed__isnull=True,
                    machine=self.machine_number,
                    thread=self.thread_id
                )
                if zombie_exports.count() > 0:
                    zombie_exports.update(
                        started=None,
                        percent_done=0,
                        machine=None,
                        thread=None,
                    )
            try:
                self.export = \
                    AuditExporter.objects.filter(completed__isnull=True, started__isnull=True, machine=None, thread=None).order_by("audit__pause",
                                                                                                        "id")[0]
                self.audit = self.export.audit
            # pylint: disable=broad-except
            except Exception as e:
            # pylint: enable=broad-except
                logger.exception(e)
                print("no audits to export at present")
                return
            self.process_export()

    # pylint: disable=too-many-statements
    def process_export(self):
        audit = self.export.audit
        self.export.started = timezone.now()
        self.export.machine = self.machine_number
        self.export.thread = self.thread_id
        self.export.save(update_fields=["started", "machine", "thread"])
        export_funcs = AuditExportApiView()
        audit_type = self.audit.params.get("audit_type_original")
        if not audit_type:
            audit_type = self.audit.audit_type
        if self.export.export_as_keywords:
            try:
                file_name, _, count = export_funcs.export_keywords(self.audit, self.audit.id, export=self.export)
            # pylint: disable=broad-except
            except Exception as e:
            # pylint: enable=broad-except
                self.export.started = None
                self.export.machine = None
                self.export.thread = None
                self.export.percent_done = 0
                self.export.save(update_fields=["started", "percent_done", "machine", "thread"])
                print("problem with exporting keywords {}, resetting audit back to 0".format(self.audit.id))
                raise Exception(e)
        elif (audit_type == 2 and not self.export.export_as_videos) \
            or (audit_type == 0 and self.export.export_as_channels):
            try:
                file_name, _ = export_funcs.export_channels(self.audit, self.audit.id, clean=self.export.clean,
                                                            export=self.export)
            # pylint: disable=broad-except
            except Exception as e:
            # pylint: enable=broad-except
                self.export.started = None
                self.export.machine = None
                self.export.thread = None
                self.export.percent_done = 0
                self.export.save(update_fields=["started", "percent_done", "machine", "thread"])
                print("problem with exporting channels {}, resetting audit back to 0".format(self.audit.id))
                raise Exception(e)
            count = AuditChannelProcessor.objects.filter(audit=self.audit)
            if self.export.clean is not None:
                count = count.filter(clean=self.export.clean)
            count = count.count()
        else:
            try:
                file_name, _ = export_funcs.export_videos(self.audit, self.audit.id, clean=self.export.clean,
                                                          export=self.export)
            # pylint: disable=broad-except
            except Exception as e:
            # pylint: enable=broad-except
                self.export.started = None
                self.export.percent_done = 0
                self.export.machine = None
                self.export.thread = None
                self.export.save(update_fields=["started", "percent_done", "machine", "thread"])
                print("problem with exporting videos {}, resetting audit back to 0".format(self.audit.id))
                raise Exception(e)
            count = AuditVideoProcessor.objects.filter(audit=self.audit)
            if self.export.clean is not None:
                count = count.filter(clean=self.export.clean)
            count = count.count()
        try:
            emails = [audit.owner.email] if audit.owner else settings.AUDIT_TOOL_EMAIL_RECIPIENTS
        # pylint: disable=broad-except
        except Exception:
        # pylint: enable=broad-except
            emails = settings.AUDIT_TOOL_EMAIL_RECIPIENTS
        try:
            self.send_audit_email(file_name, emails, count)
        except Exception as e:
            print("error sending email to {}".format(str(emails)))
        self.export.completed = timezone.now()
        self.export.file_name = file_name
        if self.audit.completed:
            self.export.final = True
        self.export.save(update_fields=["completed", "file_name", "final"])
        raise Exception("Audit {} exported with filename {}".format(self.audit.id, file_name))
    # pylint: утфиду=too-many-statements

    def send_audit_email(self, file_name, recipients, count):
        if count == 0:
            return
        file_url = AuditS3Exporter.generate_temporary_url(file_name, 604800)
        subject = "Audit '{}' Completed".format(self.audit.params.get("name"))
        body = "Audit '{}' has finished with {} results. Click " \
                   .format(self.audit.params.get("name"), "{:,}".format(count)) \
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

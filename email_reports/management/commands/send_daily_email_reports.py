import logging
import traceback

from django.conf import settings
from django.core.management.base import BaseCommand

from email_reports.reports import CampaignUnderPacing, CampaignOverPacing, \
    DailyCampaignReport, CampaignUnderMargin, TechFeeCapExceeded

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    email_report_classes = (
        DailyCampaignReport,
        CampaignUnderMargin,
        TechFeeCapExceeded,
        CampaignUnderPacing,
        CampaignOverPacing,
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.host = settings.HOST

    def add_arguments(self, parser):
        parser.add_argument(
            '--debug',
            dest='debug',
            help='Add prefix to debug emails to which the notification is sent',
            type=bool,
            default=False,
        )
        parser.add_argument(
            '--host',
            dest='host',
            help='Replace the host to show the saved emails',
        )
        parser.add_argument(
            '--reports',
            dest='reports',
            help='Pass the only report classes you want to be sent',
            type=str,
            default="",
        )
        parser.add_argument(
            '--roles',
            dest='roles',
            help='Pass the only roles which will receive reports '
                 '(report=DailyCampaignReport)',
            type=str,
            default="",
        )
        parser.add_argument(
            '--margin_bound',
            dest='margin_bound',
            help='Replace margin_bound; default is .10 (10%)',
            type=float,
        )
        parser.add_argument(
            '--pacing_bound',
            dest='pacing_bound',
            help='Replace pacing_bound; default is .10 (10%)',
            type=float,
        )
        parser.add_argument(
            '--days_to_end',
            dest='days_to_end',
            help='Replace days_to_end; default is 7',
            type=int,
        )
        parser.add_argument(
            '--fake_tech_fee_cap',
            dest='fake_tech_fee_cap',
            help='Replace margin_bound; default is .10 (10%)',
            type=str,
        )

    def handle(self, *args, **options):

        reports = list(filter(None, options.get('reports').split(',')))
        debug = options.get('debug')
        if options.get('host'):
            self.host = options.get('host')

        kwargs = dict(
            margin_bound=options.get('margin_bound'),
            days_to_end=options.get('days_to_end'),
            fake_tech_fee_cap=options.get('fake_tech_fee_cap'),
            roles=options.get('roles'),
        )

        for report_class in self.email_report_classes:
            if reports and report_class.__name__ not in reports:
                continue
            try:
                report = report_class(self.host, debug, **kwargs)
                report.send()
            except Exception as e:
                logger.critical('Worker got error: %s' % str(e))
                logger.critical(traceback.format_exc())

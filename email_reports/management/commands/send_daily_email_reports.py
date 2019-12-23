import logging

from django.core.management.base import BaseCommand

from email_reports.tasks import send_daily_email_reports

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            '--debug',
            dest='debug',
            help='Add prefix to debug emails to which the notification is sent',
            type=bool,
            default=False,
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
            help='Replace pacing_bound; default is .25 (25%)',
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
        parser.add_argument(
            "--detach",
            "-d",
            dest="detach",
            action="store_true",
            help="Run in background"
        )

    def handle(self, *args, **options):
        kwargs = dict(
            reports=list(filter(None, options.get('reports').split(','))),
            debug=options.get('debug'),
            margin_bound=options.get('margin_bound'),
            days_to_end=options.get('days_to_end'),
            fake_tech_fee_cap=options.get('fake_tech_fee_cap'),
            roles=options.get('roles'),
        )

        if options.get("detach"):
            send_daily_email_reports.delay(**kwargs)
        else:
            send_daily_email_reports(**kwargs)

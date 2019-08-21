from django.core.management.base import BaseCommand

from aw_reporting.update.update_salesforce_data import update_salesforce_data


class Command(BaseCommand):
    debug_update = False
    prev_month_flight_write_stop_day = 3

    def add_arguments(self, parser):
        parser.add_argument(
            '--no_update',
            dest='no_update',
            default=False,
            type=bool,
            help='Do not update any data on salesforce'
        )

        parser.add_argument(
            '--no_get',
            dest='no_get',
            default=False,
            type=bool,
            help='Do not get any data from salesforce'
        )

        parser.add_argument(
            '--debug_update',
            dest='debug_update',
            default=False,
            type=bool,
            help='Do not update any data on salesforce, just write to the log'
        )

        parser.add_argument("--force", "-f", dest="force", action="store_true", help="Update for whole period")
        parser.add_argument("--no-fl", dest="no_flights", action="store_true", help="Update excluding flights")
        parser.add_argument("--no-pl", dest="no_placements", action="store_true", help="Update excluding placements")
        parser.add_argument("--no-opp", dest="no_opportunities", action="store_true",
                            help="Update excluding opportunities")
        parser.add_argument("--opp", dest="opportunities", action="append", help="Opportunity ids for updating")

    def handle(self, *args, **options):
        signature = update_salesforce_data.si(
            do_get=not options.get("no_get", False),
            do_update=not options.get("no_update", False),
            debug_update=options.get("debug_update", False),
            opportunity_ids=options.get("opportunities", None),
            force_update=options.get("force", False),
            skip_flights=options.get("no_flights", False),
            skip_placements=options.get("no_placements", False),
            skip_opportunities=options.get("no_opportunities", False),
        )
        signature.apply_async()

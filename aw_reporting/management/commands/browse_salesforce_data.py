import calendar
import logging
from datetime import datetime
from datetime import timedelta

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import IntegrityError
from django.db.models import F
from django.db.models import OuterRef
from django.db.models import Q
from django.db.models import Subquery

from aw_reporting.models.ad_words import Campaign
from aw_reporting.models.salesforce import Activity
from aw_reporting.models.salesforce import Category
from aw_reporting.models.salesforce import Contact
from aw_reporting.models.salesforce import Flight
from aw_reporting.models.salesforce import OpPlacement
from aw_reporting.models.salesforce import Opportunity
from aw_reporting.models.salesforce import SFAccount
from aw_reporting.models.salesforce import User
from aw_reporting.models.salesforce import UserRole
from aw_reporting.models.salesforce_constants import DynamicPlacementType
from aw_reporting.models.salesforce_constants import SalesForceGoalType
from aw_reporting.reports.pacing_report import PacingReport
from aw_reporting.reports.pacing_report import get_pacing_from_flights
from aw_reporting.salesforce import Connection as SConnection
from utils.cache import cache_reset
from utils.datetime import now_in_default_tz
from utils.lang import almost_equal

logger = logging.getLogger(__name__)

stop_words = ["Q%d" % i for i in range(1, 5)] \
             + list(calendar.month_name) \
             + ["FY", "Campaign"]

WRITE_START = datetime(2016, 9, 1).date()


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
        try:
            self.debug_update = options.get('debug_update')

            sc = None
            if not options.get('no_get'):
                sc = SConnection()
                self.get(sc)

            if not options.get('no_update'):
                sc = sc or SConnection()
                self.update(sc, options)

            self.match_using_placement_numbers()
            cache_reset()

        except Exception as ex:
            logger.exception(ex)

    @staticmethod
    def match_using_placement_numbers():
        placements = OpPlacement.objects.filter(
            number=OuterRef("placement_code")).values("pk")[:1]
        campaigns = Campaign.objects \
            .filter(placement_code__isnull=False) \
            .annotate(placement_id=Subquery(placements))
        count = campaigns.update(salesforce_placement_id=F("placement_id"))
        if not settings.IS_TEST:
            logger.info("Matched %d Campaigns" % count)

    def update(self, sc, options):
        opportunity_ids = options.get("opportunities", None)
        force = options.get("force")
        if not options.get("no_placements"):
            self._update_placements(sc, opportunity_ids)

        if not options.get("no_opportunities"):
            self._update_opportunities(sc, opportunity_ids)

        if not options.get("no_flights"):
            self._update_flights(sc, force, opportunity_ids)

    def _update_flights(self, sc, force, opportunity_ids):
        opp_filter = Q(placement__opportunity__number__in=opportunity_ids) if opportunity_ids else Q()
        pacing_report = PacingReport()
        for flight in self.flights_to_update_qs(force).filter(opp_filter):

            units, cost = flight.delivered_units, flight.delivered_cost
            flight_data = pacing_report.get_flights_data(id=flight.id)
            pacing = get_pacing_from_flights(flight_data)
            pacing = pacing * 100 if pacing is not None else pacing

            update = {}
            if units != flight.delivered:
                # 0 is not an acceptable value for this field
                units = units or None
                update['Delivered_Ad_Ops__c'] = units
            if cost != flight.cost:
                update['Total_Flight_Cost__c'] = cost

            if ((pacing is None) ^ (flight.pacing is None)) \
                    or (pacing is not None and not almost_equal(pacing, flight.pacing)):
                update['Pacing__c'] = pacing

            if update:
                try:
                    r = 204 if self.debug_update else sc.sf.Flight__c.update(
                        flight.id, update)
                except Exception as e:
                    logger.critical("Unhandled exception: %s" % str(e))
                else:
                    if r == 204:
                        logger.debug(
                            'Flight %s %s %s was updated: %s' % (
                                flight.id, str(flight.start),
                                str(flight.placement.goal_type_id), str(update)
                            )
                        )
                    else:
                        logger.critical(
                            'Update Error: %s %s' % (flight.id, str(r))
                        )
        # Service Fee Dynamic Placement
        # When the flight is created, IQ needs to put a 0 for costs and 1
        # for delivered units on each of the flights
        service_flights_to_update = Flight.objects.filter(
            placement__dynamic_placement=DynamicPlacementType.SERVICE_FEE,
        ) \
            .filter(opp_filter) \
            .exclude(delivered=1, cost=0,)
        for flight in service_flights_to_update:
            update = dict(Delivered_Ad_Ops__c=1, Total_Flight_Cost__c=0)
            try:
                r = 204 if self.debug_update else sc.sf.Flight__c.update(
                    flight.id, update)
            except Exception as e:
                logger.critical("Unhandled exception: %s" % str(e))
            else:
                if r == 204:
                    logger.info(
                        'Flight %s %s %s was updated: %s' % (
                            flight.id, str(flight.start),
                            str(flight.placement.goal_type_id), str(update)
                        )
                    )
                else:
                    logger.critical(
                        'Update Error: %s %s' % (flight.id, str(r))
                    )

    def _update_opportunities(self, sc, opportunity_ids):
        opp_filter = Q(number__in=opportunity_ids) if opportunity_ids else Q()
        for opportunity in Opportunity.objects.filter(opp_filter):
            update = {}
            ids = Campaign.objects.filter(
                salesforce_placement__opportunity=opportunity).values_list(
                'account_id',
                flat=True).distinct()

            aw_cid = ",".join(filter(lambda x: x is not None, ids))
            if not aw_cid:
                continue

            if opportunity.aw_cid != aw_cid:
                update['AdWords_CID__c'] = aw_cid

            if update:
                try:
                    r = 204 if self.debug_update else sc.sf.Opportunity.update(
                        opportunity.id, update)
                except Exception as e:
                    logger.critical("Unhandled exception: %s" % str(e))
                else:
                    if r == 204:
                        logger.info(
                            'Opportunity %s was updated: %s' % (
                                opportunity.id, str(update)
                            )
                        )
                    else:
                        logger.critical(
                            'Update Error: %s %s' % (
                                opportunity.id, str(r)
                            )
                        )

    def _update_placements(self, sc, opportunity_ids):
        opp_filter = Q(opportunity__number__in=opportunity_ids) if opportunity_ids else Q()
        for placement in OpPlacement.objects.filter(opp_filter) \
                .filter(adwords_campaigns__isnull=False) \
                .distinct():

            aw_pl = placement.adwords_campaigns.order_by('id').first().name
            if placement.ad_words_placement != aw_pl:
                update = {'Adwords_Placement_IQ__c': aw_pl}
                try:
                    r = 204 if self.debug_update else sc.sf.Placement__c.update(
                        placement.id,
                        update,
                    )
                except Exception as e:
                    logger.critical("Unhandled exception: %s" % str(e))
                else:
                    if r == 204:
                        logger.info(
                            'Placement %s %s was updated: %s' % (
                                placement.id, placement.name, str(update)
                            )
                        )
                    else:
                        logger.critical(
                            'Update Error: %s %s' % (
                                placement.id, str(r)
                            )
                        )

    def flights_to_update_qs(self, force):
        today = now_in_default_tz().date()

        if force:
            date_filters = Q()
        else:
            date_filters = Q(start__lte=today, end__gte=today - timedelta(days=settings.SALESFORCE_UPDATE_DELAY_DAYS))

        type_filters = Q(placement__goal_type_id__in=(
            SalesForceGoalType.CPM, SalesForceGoalType.CPV)) \
                       | Q(placement__dynamic_placement__in=(
            DynamicPlacementType.BUDGET,
            DynamicPlacementType.RATE_AND_TECH_FEE))
        flights = Flight.objects.filter(
            start__gte=WRITE_START,
            placement__adwords_campaigns__isnull=False,
        ) \
            .exclude(
            placement__dynamic_placement=DynamicPlacementType.SERVICE_FEE) \
            .filter(type_filters) \
            .filter(date_filters) \
            .prefetch_related(
            "placement").distinct()
        return flights

    @staticmethod
    def get(sc):
        opportunity_ids = []
        placement_ids = []
        for model, method in [
            (UserRole, 'get_user_roles'),
            (User, 'get_users'),
            (Contact, 'get_contacts'),
            (SFAccount, 'get_accounts'),
            (Category, 'get_categories'),
            (Opportunity, 'get_opportunities'),
            (OpPlacement, 'get_placements'),
            (Flight, 'get_flights'),
            (Activity, 'get_activities'),
        ]:
            logger.info("Getting %s items" % model.__name__)
            existed_ids = model.objects.all().values_list('id', flat=True)

            item_ids = []
            insert_list = []
            update = 0
            for item_data in getattr(sc, method)():
                # save ony children of saved parents
                if method == 'get_placements' \
                        and item_data['Insertion_Order__c'] \
                        not in opportunity_ids:
                    continue
                if method == 'get_flights' \
                        and item_data['Placement__c'] not in placement_ids:
                    continue
                data = model.get_data(item_data)

                # save items
                item_id = data['id']
                item_ids.append(item_id)

                if item_id in existed_ids:
                    del data['id']
                    try:
                        item = model.objects.get(pk=item_id)
                        for key, value in data.items():
                            setattr(item, key, value)
                        item.save()
                    except IntegrityError as e:
                        logger.exception(e)
                    update += 1
                else:
                    insert_list.append(
                        model(**data)
                    )
            if insert_list:
                model.objects.safe_bulk_create(insert_list)
                logger.info('   Inserted new %d items' % len(insert_list))
            logger.info('   Updated %d items' % update)

            # delete items
            deleted_ids = set(existed_ids) - set(item_ids)
            if deleted_ids:
                model.objects.filter(pk__in=deleted_ids).delete()
                logger.info('   Deleted %d items' % len(deleted_ids))

            # save parent ids
            if method == 'get_opportunities':
                opportunity_ids = item_ids
            elif method == 'get_placements':
                placement_ids = item_ids

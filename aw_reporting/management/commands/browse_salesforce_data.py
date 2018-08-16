import calendar
import logging
import traceback
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
from aw_reporting.salesforce import Connection as SConnection
from utils.cache import cache_reset
from utils.datetime import now_in_default_tz

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
            '--opp_coe',
            dest='opp_coe',
            default=2.,
            type=float,
            help='Similarity greater or equal'
                 ' this number for auto-matching'
        )

        parser.add_argument(
            '--debug_update',
            dest='debug_update',
            default=False,
            type=bool,
            help='Do not update any data on salesforce, just write to the log'
        )

        parser.add_argument(
            '--prev_month_flight_write_stop_day',
            dest='prev_month_flight_write_stop_day',
            default=3,
            type=int,
            help='Using this option we can debug writing'
                 ' to the previous month flights'
        )

    def handle(self, *args, **options):
        try:
            self.debug_update = options.get('debug_update')
            self.prev_month_flight_write_stop_day = options.get(
                'prev_month_flight_write_stop_day')

            sc = None
            if not options.get('no_get'):
                sc = SConnection()
                self.get(sc)

            if not options.get('no_update'):
                sc = sc or SConnection()
                self.update(sc)

            self.match_using_placement_numbers()
            cache_reset()

        except Exception:
            logger.critical(traceback.format_exc())

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

    def update(self, sc):

        for placement in OpPlacement.objects.filter(
                adwords_campaigns__isnull=False).distinct():

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

        for opportunity in Opportunity.objects.all():
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

        for flight in self.flights_to_update_qs:

            units, cost = flight.delivered_units, flight.delivered_cost

            update = {}
            if units != flight.delivered:
                # 0 is not an acceptable value for this field
                units = units or None
                update['Delivered_Ad_Ops__c'] = units
            if cost != flight.cost:
                update['Total_Flight_Cost__c'] = cost

            if update:
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

        # Service Fee Dynamic Placement
        # When the flight is created, IQ needs to put a 0 for costs and 1
        # for delivered units on each of the flights
        service_flights_to_update = Flight.objects.filter(
            placement__dynamic_placement=DynamicPlacementType.SERVICE_FEE,
        ).exclude(
            delivered=1, cost=0,
        )
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

    @property
    def flights_to_update_qs(self):
        now = now_in_default_tz()

        date_filters = Q(start__lte=now, end__gte=now)

        stop_updating_date = self.prev_month_flight_write_stop_day
        if now.hour > 5:
            stop_updating_date -= 1

        if now.day <= stop_updating_date:
            prev_month_date = now.replace(day=1) - timedelta(days=1)
            date_filters |= Q(end__month=prev_month_date.month,
                              end__year=prev_month_date.year)

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
            # (UserRole, 'get_user_roles'),
            # (User, 'get_users'),
            # (Contact, 'get_contacts'),
            # (SFAccount, 'get_accounts'),
            # (Category, 'get_categories'),
            (Opportunity, 'get_opportunities'),
            # (OpPlacement, 'get_placements'),
            # (Flight, 'get_flights'),
            # (Activity, 'get_activities'),
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
                        model.objects.filter(pk=item_id).update(**data)
                    except IntegrityError as e:
                        logging.critical(e)
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

import logging
from datetime import datetime
from datetime import timedelta
import time

from django.conf import settings
from django.db import IntegrityError
from django.db.models import F
from django.db.models import OuterRef
from django.db.models import Q
from django.db.models import Subquery
from django.db.models.signals import pre_save
import pytz

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
from saas import celery_app
from saas.configs.celery import TaskExpiration
from saas.configs.celery import TaskTimeout
from utils.datetime import now_in_default_tz
from utils.lang import almost_equal

__all__ = [
    "update_salesforce_data"
]

logger = logging.getLogger(__name__)
WRITE_START = datetime(2016, 9, 1).date()


@celery_app.task(expires=TaskExpiration.FULL_SF_UPDATE, soft_time_limit=TaskTimeout.FULL_SF_UPDATE)
def update_salesforce_data(do_delete=True, do_get=True, do_update=True, debug_update=False, opportunity_ids=None, force_update=False,
                           skip_flights=False, skip_placements=False, skip_opportunities=False, delete_from_days=14):
    logger.info("Salesforce update started")
    start = time.time()
    today = now_in_default_tz().date()
    sc = None
    if do_delete:
        sc = SConnection()
        perform_delete(sc=sc, delete_from_days=delete_from_days)

    if do_get:
        sc = sc or SConnection()
        perform_get(sc=sc)

    if do_update:
        sc = sc or SConnection()
        perform_update(sc=sc, today=today, opportunity_ids=opportunity_ids, force_update=force_update,
                       skip_placements=skip_placements, skip_opportunities=skip_opportunities,
                       debug_update=debug_update, skip_flights=skip_flights)

    match_using_placement_numbers()
    logger.info(f"Salesforce update finished. Took: {time.time() - start}")


def perform_delete(sc, delete_from_days):
    today = datetime.now(pytz.UTC)
    from_deleted = today - timedelta(days=delete_from_days)

    for model, resource in [
        (Opportunity, "Opportunity"),
        (OpPlacement, "Placement__c"),
        (Flight, "Flight__c"),
    ]:
        deleted_items = sc.get_deleted_items(resource, from_deleted, today)
        to_delete = [record["id"] for record in deleted_items]
        if to_delete:
            model.objects.filter(id__in=to_delete).delete()
            logger.debug(f"Deleted {model.__name__} entries: {to_delete}")


def perform_get(sc):
    opportunity_ids = set()
    placement_ids = set()
    end_date_threshold = datetime.today().date() - timedelta(days=14)
    for model, method, condition in [
        (UserRole, 'get_user_roles', None),
        (User, 'get_users', None),
        (Contact, 'get_contacts', None),
        (SFAccount, 'get_accounts', None),
        (Category, 'get_categories', None),
        (Opportunity, 'get_opportunities', f"MAX_Placement_End_Date__c > {end_date_threshold}"),
        (OpPlacement, 'get_placements', f"Placement_End_Date__c > {end_date_threshold}"),
        (Flight, 'get_flights', f"Flight_End_Date__c > {end_date_threshold}"),
        (Activity, 'get_activities', None),
    ]:
        logger.debug("Getting %s items" % model.__name__)
        to_create = []
        to_update = []
        rows = getattr(sc, method)(where=condition)
        existing_items = {
            item.id: item for item in model.objects.all()
        }
        update_fields = None
        for item_data in rows:
            # save ony children of saved parents
            if method == 'get_placements' \
                    and item_data['Insertion_Order__c'] \
                    not in opportunity_ids:
                continue
            if method == 'get_flights' \
                    and item_data['Placement__c'] not in placement_ids:
                continue
            data = model.get_data(item_data)
            if update_fields is None:
                update_fields = [key for key in data.keys() if key != "id"]
            item_id = data['id']
            existing = existing_items.get(item_id)
            if existing is None:
                to_create.append(model(**data))
                continue
            # Compare values since Contact and SFAccount models are easily compared and contain many entries
            if method == "get_contacts" or method == "get_accounts":
                existing_values = set(getattr(existing, key) for key in data.keys())
                if existing_values != set(data.values()):
                    to_update.append(model(**data))
            elif method == "get_categories":
                try:
                    Category.objects.get(id=data["id"])
                except IntegrityError:
                    to_create.append(Category(**data))
            else:
                to_update.append(model(**data))
        if to_update:
            # send pre_save signals for notifications
            for item in to_update:
                pre_save.send(model, instance=item)
            model.objects.bulk_update(to_update, fields=update_fields, batch_size=1000)
            logger.debug(f"Updated {len(to_update)} items for: {model}")
        if to_create:
            model.objects.safe_bulk_create(to_create)
            logger.debug(f"Created {len(to_create)} items for: {model}")

        # save parent ids
        if method == 'get_opportunities':
            opportunity_ids = set(Opportunity.objects.all().values_list("id", flat=True))
        elif method == 'get_placements':
            placement_ids = set(OpPlacement.objects.all().values_list("id", flat=True))


def perform_update(sc, today, opportunity_ids, force_update, skip_placements, skip_opportunities,
                   skip_flights, debug_update):
    if not skip_placements:
        update_placements(sc=sc, opportunity_ids=opportunity_ids, debug_update=debug_update)

    if not skip_opportunities:
        update_opportunities(sc=sc, opportunity_ids=opportunity_ids, debug_update=debug_update)

    if not skip_flights:
        update_flights(sc=sc, force_update=force_update, opportunity_ids=opportunity_ids, today=today,
                       debug_update=debug_update)


def update_opportunities(sc, opportunity_ids, debug_update):
    opp_filter = Q(number__in=opportunity_ids) if opportunity_ids else Q()
    opportunities = Opportunity.objects.filter(opp_filter) \
        .difference(Opportunity.demo_items())
    for opportunity in opportunities:
        update = {}
        ids = Campaign.objects.filter(
            salesforce_placement__opportunity=opportunity).values_list(
            'account_id',
            flat=True).distinct()

        aw_cid = ",".join([str(campaign_id) for campaign_id in filter(lambda x: x is not None, ids)])
        if not aw_cid:
            continue

        if opportunity.aw_cid != aw_cid:
            update['AdWords_CID__c'] = aw_cid

        if update:
            try:
                r = 204 \
                    if debug_update \
                    else sc.sf.Opportunity.update(opportunity.id, update)
            except Exception as e:
                if getattr(e, "status", 0) == 404:
                    logger.info(str(e))
                else:
                    logger.critical("Unhandled exception: %s" % str(e))
            else:
                if r == 204:
                    logger.debug(
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


def update_placements(sc, opportunity_ids, debug_update):
    opp_filter = Q(opportunity__number__in=opportunity_ids) if opportunity_ids else Q()
    placements = OpPlacement.objects.filter(opp_filter) \
        .filter(adwords_campaigns__isnull=False) \
        .difference(OpPlacement.demo_items()) \
        .distinct()
    for placement in placements:

        aw_pl = placement.adwords_campaigns.order_by('id').first().name
        if placement.ad_words_placement != aw_pl:
            update = {'Adwords_Placement_IQ__c': aw_pl}
            try:
                r = 204 \
                    if debug_update \
                    else sc.sf.Placement__c.update(placement.id, update, )
            except Exception as e:
                if getattr(e, "status", 0) == 404:
                    logger.info(str(e))
                else:
                    logger.critical("Unhandled exception: %s" % str(e))
            else:
                if r == 204:
                    logger.debug(
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


def update_flights(sc, force_update, opportunity_ids, today, debug_update):
    opp_filter = Q(placement__opportunity__number__in=opportunity_ids) if opportunity_ids else Q()
    pacing_report = PacingReport()
    for flight in flights_to_update_qs(force_update, today).filter(opp_filter):

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
                r = 204 if debug_update else sc.sf.Flight__c.update(flight.id, update)
            except Exception as e:
                if getattr(e, "status", 0) == 404:
                    logger.info(str(e))
                else:
                    try:
                        message = f"{flight.name}: {flight.placement.opportunity.ad_ops_manager}\n"
                    except (AttributeError, OpPlacement.DoesNotExist, Opportunity.DoesNotExist):
                        message = str(e)
                    logger.critical("Unhandled exception: %s" % message)
            else:
                if r == 204:
                    logger.debug(
                        'Flight %s %s %s was updated: %s' % (
                            flight.id, str(flight.sstart),
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
        .exclude(delivered=1, cost=0, )
    for flight in service_flights_to_update:
        update = dict(Delivered_Ad_Ops__c=1, Total_Flight_Cost__c=0)
        try:
            r = 204 if debug_update else sc.sf.Flight__c.update(flight.id, update)
        except Exception as e:
            if getattr(e, "status", 0) == 404:
                logger.warning(str(e))
            else:
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


def flights_to_update_qs(force_update, today):
    if force_update:
        date_filters = Q()
    else:
        date_filters = Q(
            start__lte=today,
            end__gte=today - timedelta(days=settings.SALESFORCE_UPDATE_DELAY_DAYS)
        )

    regular_placement = Q(placement__goal_type_id__in=(SalesForceGoalType.CPM, SalesForceGoalType.CPV))
    dynamic_placement = Q(placement__dynamic_placement__in=(DynamicPlacementType.BUDGET,
                                                            DynamicPlacementType.RATE_AND_TECH_FEE))
    type_filters = regular_placement | dynamic_placement
    flights = Flight.objects.filter(
        start__gte=WRITE_START,
        placement__adwords_campaigns__isnull=False,
    ) \
        .exclude(placement__dynamic_placement=DynamicPlacementType.SERVICE_FEE) \
        .filter(type_filters) \
        .filter(date_filters) \
        .difference(Flight.demo_items()) \
        .prefetch_related("placement") \
        .distinct()
    return flights


def match_using_placement_numbers():
    placements = OpPlacement.objects.filter(
        number=OuterRef("placement_code")).values("pk")[:1]
    campaigns = Campaign.objects \
        .filter(placement_code__isnull=False) \
        .annotate(placement_id=Subquery(placements))
    count = campaigns.update(salesforce_placement_id=F("placement_id"))
    from django.conf import settings
    if not settings.IS_TEST:
        logger.debug("Matched %d Campaigns" % count)

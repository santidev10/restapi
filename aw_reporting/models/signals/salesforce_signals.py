from math import floor

# pylint: disable=cyclic-import
from django.utils import timezone

from aw_reporting.models.salesforce import Flight
from aw_reporting.models.salesforce import OpPlacement
from aw_reporting.models.salesforce import Alert
from aw_reporting.models.salesforce_constants import FlightAlert
from aw_reporting.models.salesforce_constants import PlacementAlert

__all__ = [
    "pre_save_flight_receiver",
    "pre_save_placement_receiver"
]


def pre_save_flight_receiver(instance, **_):
    if Flight.objects.filter(pk=instance.pk).exists():
        old_flight = Flight.objects.get(pk=instance.pk)
        flight_changed(old_flight, instance)


def flight_changed(old_flight: Flight, new_flight: Flight):
    if old_flight.placement.opportunity.probability == 100:
        flight_ordered_units_changed(old_flight, new_flight)
        flight_total_cost_changed(old_flight, new_flight)
        flight_dates_changed(old_flight, new_flight)


def flight_ordered_units_changed(old_flight: Flight, new_flight: Flight):
    old_ordered_units = old_flight.ordered_units
    new_ordered_units = new_flight.ordered_units
    if old_ordered_units is None or new_ordered_units is None:
        return
    ordered_units_changed = floor(old_ordered_units) != floor(new_ordered_units)
    if not ordered_units_changed or new_flight.placement.dynamic_placement is not None:
        return
    placement = new_flight.placement
    opportunity = placement.opportunity
    ad_ops = opportunity.ad_ops_manager

    # pylint: disable=import-outside-toplevel
    from email_reports.reports.flight_ordered_units_changed import FlightOrderedUnitsChangedEmail
    # pylint: enable=import-outside-toplevel
    email = FlightOrderedUnitsChangedEmail(
        opportunity_name=opportunity.name,
        placement_name=placement.name,
        flight_name=new_flight.name,
        old_ordered_units=old_flight.ordered_units,
        new_ordered_units=new_flight.ordered_units,
        recipients=[ad_ops.email] if ad_ops else None,
    )
    email.send()


def flight_total_cost_changed(old_flight: Flight, new_flight: Flight):
    total_cost_changed = old_flight.total_cost != new_flight.total_cost
    if not total_cost_changed or new_flight.placement.dynamic_placement is None:
        return

    placement = new_flight.placement
    opportunity = placement.opportunity
    ad_ops = opportunity.ad_ops_manager
    # pylint: disable=import-outside-toplevel
    from email_reports.reports.flight_total_cost_changed import FlightTotalCostChangedEmail
    # pylint: enable=import-outside-toplevel
    email = FlightTotalCostChangedEmail(
        opportunity_name=opportunity.name,
        placement_name=placement.name,
        flight_name=new_flight.name,
        old_total_cost=old_flight.total_cost,
        new_total_cost=new_flight.total_cost,
        recipients=[ad_ops.email] if ad_ops else None,
    )
    email.send()


def flight_dates_changed(old_flight: Flight, new_flight: Flight):
    dates_changed = old_flight.start != new_flight.start or old_flight.end != new_flight.end
    if dates_changed:
        try:
            Alert.objects.update_or_create(
                record_id=new_flight.id,
                code=FlightAlert.DATES_CHANGED.value,
                defaults=dict(
                    message=f"{old_flight.start.strftime('%Y-%m-%d')} - {old_flight.end.strftime('%Y-%m-%d')} "
                            f"to {new_flight.start.strftime('%Y-%m-%d')} - {new_flight.end.strftime('%Y-%m-%d')}"
                )
            )
        except AttributeError:
            pass


def pre_save_placement_receiver(instance, **_):
    if OpPlacement.objects.filter(pk=instance.pk).exists():
        old_placement = OpPlacement.objects.get(pk=instance.pk)
        placement_changed(old_placement, instance)


def placement_changed(old_placement: OpPlacement, new_placement: OpPlacement):
    old_ordered_units = old_placement.ordered_units
    new_ordered_units = new_placement.ordered_units
    if old_ordered_units is not None and new_ordered_units is not None and \
            floor(old_placement.ordered_units) != floor(new_placement.ordered_units):
        today = timezone.now().date()
        Alert.objects.update_or_create(
            record_id=new_placement.id,
            code=PlacementAlert.ORDERED_UNITS_CHANGED.value,
            defaults=dict(
                message=f"{old_placement.ordered_units} "
                        f"to {new_placement.ordered_units} on {today.strftime('%Y-%m-%d')}"
            )
        )

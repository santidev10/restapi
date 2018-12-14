from aw_reporting.models.salesforce import Flight

__all__ = [
    "pre_save_flight_receiver",
]


def pre_save_flight_receiver(instance, **_):
    if Flight.objects.filter(pk=instance.pk).exists():
        old_flight = Flight.objects.get(pk=instance.pk)
        flight_changed(old_flight, instance)


def flight_changed(old_flight: Flight, new_flight: Flight):
    if old_flight.placement.opportunity.probability == 100:
        flight_ordered_units_changed(old_flight, new_flight)
        flight_total_cost_changed(old_flight, new_flight)


def flight_ordered_units_changed(old_flight: Flight, new_flight: Flight):
    ordered_units_changed = old_flight.ordered_units != new_flight.ordered_units
    if not ordered_units_changed or new_flight.placement.dynamic_placement is not None:
        return
    placement = new_flight.placement
    opportunity = placement.opportunity
    ad_ops = opportunity.ad_ops_manager

    from email_reports.reports.flight_ordered_units_changed import FlightOrderedUnitsChangedEmail
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
    from email_reports.reports.flight_total_cost_changed import FlightTotalCostChangedEmail
    email = FlightTotalCostChangedEmail(
        opportunity_name=opportunity.name,
        placement_name=placement.name,
        flight_name=new_flight.name,
        old_total_cost=old_flight.total_cost,
        new_total_cost=new_flight.total_cost,
        recipients=[ad_ops.email] if ad_ops else None,
    )
    email.send()

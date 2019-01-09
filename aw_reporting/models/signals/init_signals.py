from django.db.models.signals import pre_save


def init_signals():
    init_flight_signals()


def init_flight_signals():
    from aw_reporting.models.salesforce import Flight
    from aw_reporting.models.signals.salesforce_signals import pre_save_flight_receiver

    pre_save.connect(pre_save_flight_receiver, sender=Flight, dispatch_uid="pre_save_flight_receiver")

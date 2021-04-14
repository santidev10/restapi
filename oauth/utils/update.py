import datetime


def get_to_update(model, ids: list, update_threshold: datetime.datetime) -> list:
    """
    Retrieve ids to update that have not been updated within threshold.
    :param model: Django app model
    :param ids: List of model ids to check if exists and are outdated
    :param update_threshold: datetime -> Lowest acceptable update time to update accounts again
    :return: list -> model ids to retrieve OAuth data for
    """
    exists = model.objects.filter(id__in=ids).values_list("id", flat=True)
    to_update = set(exists.filter(updated_at__lte=update_threshold))
    to_create = set(ids) - set(exists)
    return [*to_create, *to_update]

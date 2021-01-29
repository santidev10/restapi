from django.contrib.auth import get_user_model

from audit_tool.models import BlacklistItem


def get_context(item_ids):
    """
    Get serializer context with item_ids
    Adds user email to blacklist data
    """
    blacklist_qs = BlacklistItem.objects.filter(item_id__in=item_ids)
    name_by_user_id: dict = {
        user.id: f"{user.first_name} {user.last_name}" for user
        in get_user_model().objects.filter(id__in=list(blacklist_qs.values_list("processed_by_user_id", flat=True)))
    }
    blacklist_data = {}
    for item in blacklist_qs:
        try:
            setattr(item, "name", name_by_user_id[item.processed_by_user_id])
        except KeyError:
            pass
        finally:
            blacklist_data[item.item_id] = item
    context = {"blacklist_data": blacklist_data}
    return context

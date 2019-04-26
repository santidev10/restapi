from django.conf import settings


def is_apex_user(email):
    return settings.CUSTOM_AUTH_FLAGS.get(email) \
           and settings.CUSTOM_AUTH_FLAGS[email].get("is_apex") is True


def is_correct_apex_domain(request_origin):
    return settings.APEX_HOST in request_origin


def check_set_superuser(user):
    try:
        if user.is_superuser:
            print('eh')





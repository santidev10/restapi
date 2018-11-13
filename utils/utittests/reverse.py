from django.urls import reverse as django_reverse


def reverse(view_name, namespaces, **kwargs):
    return django_reverse(
        ":".join(namespaces + [view_name]),
        **kwargs
    )

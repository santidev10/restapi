from django.http import QueryDict
from django.urls import reverse as django_reverse


def reverse(view_name, namespaces, query_params=None, **kwargs):
    url = django_reverse(
        ":".join(namespaces + [view_name]),
        **kwargs
    )
    if query_params is not None:
        query_dict = build_query_dict(query_params)
        url = "?".join([url, query_dict.urlencode()])

    return url


def build_query_dict(query_params_dict):
    query_params = QueryDict(mutable=True)
    query_params.update(**query_params_dict)
    return query_params

from functools import reduce

from django.db.models import Q, When, Count


class Operator:
    AND = "AND"
    OR = "OR"


def build_query(expressions, condition):
    condition_upper = condition.upper()
    if condition_upper == Operator.AND:
        return AND(*expressions)
    elif condition_upper == Operator.OR:
        return OR(*expressions)
    return Q()


def build_query_value(queryset, field, values, condition):
    in_query = Q(**{field + "__in": values})
    if condition == Operator.OR:
        return queryset.filter(in_query)
    elif condition == Operator.AND:
        temp_queryset = queryset.annotate(count=Count(in_query)) \
            .filter(count=len(values))
        pks = temp_queryset.values_list("pk", flat=True)

        return queryset.filter(pk__in=pks)


def build_query_bool(fields, condition, value=True):
    expressions = [{f: value} for f in fields]
    return build_query(expressions, condition)


def OR(*args):
    return reduce(lambda r, f: r | Q(**f), args, Q())


def AND(*args):
    return reduce(lambda r, f: r & Q(**f), args, Q())


def split_request(initial_queryset, filters, chunk_size):
    chunks = [filters[i:i + chunk_size]
              for i in range(0, len(filters), chunk_size)]
    queryset = initial_queryset
    for chunk in chunks:
        changed = False
        for f in chunk:
            queryset, current_changed = f(queryset)
            changed |= current_changed
        if changed:
            ids = (t[0] for t in queryset.values_list("id"))
            queryset = initial_queryset.filter(id__in=ids)
    return queryset


def merge_when(date_filter, **kwargs):
    return [When(**(date_filter or dict(pk__isnull=False)), **kwargs)
            for date_filter in date_filter]

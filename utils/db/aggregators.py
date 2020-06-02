from django.db.models import Aggregate


# pylint: disable=abstract-method
class ConcatAggregate(Aggregate):
    function = "array_agg"
    name = "Concat"
    template = "%(function)s(%(distinct)s%(expressions)s)"
    allow_distinct = True

    def __init__(self, expression, distinct=False, **extra):
        super(ConcatAggregate, self).__init__(
            expression,
            distinct="DISTINCT " if distinct else "",
            **extra
        )

    def as_sqlite(self, compiler, connection, *args, **kwargs):
        return super(ConcatAggregate, self).as_sql(
            compiler, connection,
            template="GROUP_CONCAT(%(distinct)s%(expressions)s)",
            *args, **kwargs
        )

    # pylint: disable=arguments-differ
    def convert_value(self, value, *args, **kwargs):
        if value is None:
            return ""
        if isinstance(value, str):
            value = value.split(",")
        if isinstance(value, list):
            value = ", ".join(str(i) for i in value if i is not None)
        return value
    # pylint: enable=arguments-differ
# pylint: enable=abstract-method

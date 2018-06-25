from django.db.models import Aggregate


class ConcatAggregate(Aggregate):
    function = 'array_agg'
    name = 'Concat'
    template = '%(function)s(%(distinct)s%(expressions)s)'

    def __init__(self, expression, distinct=False, **extra):
        super(ConcatAggregate, self).__init__(
            expression,
            distinct='DISTINCT ' if distinct else '',
            **extra
        )

    def as_sqlite(self, compiler, connection, *args, **kwargs):
        return super(ConcatAggregate, self).as_sql(
            compiler, connection,
            template="GROUP_CONCAT(%(distinct)s%(expressions)s)",
            *args, **kwargs
        )

    def convert_value(self, value, expression, connection, context):
        if value is None:
            return ""
        if type(value) is str:
            value = value.split(",")
        if type(value) is list:
            value = ", ".join(str(i) for i in value if i is not None)
        return value
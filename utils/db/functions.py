from django.db.models import DateField
from django.db.models.functions.datetime import TruncBase


class TruncQuarter(TruncBase):
    kind = 'quarter'


# todo: use the same class from django (since Django 2.1)
class TruncWeek(TruncBase):
    kind = 'week'

    def as_sql(self, compiler, connection):
        if isinstance(self.output_field, DateField):
            inner_sql, inner_params = compiler.compile(self.lhs)
            sql = connection.ops.date_trunc_sql(self.kind, inner_sql)
            return sql, inner_params
        else:
            return super(TruncWeek, self).as_sql(compiler, connection)

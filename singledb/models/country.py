from django.contrib.postgres.fields import JSONField
from django.db import models


class Country(models.Model):
    code = models.CharField(max_length=10, primary_key=True)
    common = models.CharField(max_length=50)
    official = models.CharField(max_length=255)
    raw_data = JSONField(null=True)

    class Meta:
        db_table = 'utils_country'
        managed = False

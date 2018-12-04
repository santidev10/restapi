from django.db import models


class BadWord(models.Model):
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=80)
    category = models.CharField(max_length=80)

    class Meta:
        unique_together = ("name", "category")

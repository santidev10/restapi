# Generated by Django 3.0.4 on 2020-05-18 17:06

from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ("aw_reporting", "0089_auto_20200514_0030"),
    ]

    operations = [
        migrations.AddField(
            model_name="opportunity",
            name="ias_campaign_name",
            field=models.CharField(default=None, max_length=250, null=True),
        ),
    ]

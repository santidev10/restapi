# Generated by Django 2.2.4 on 2019-10-10 16:22

from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ("aw_reporting", "0061_auto_20190926_1658"),
    ]

    operations = [
        migrations.AddField(
            model_name="adgroup",
            name="device_tv_screens",
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name="campaign",
            name="device_tv_screens",
            field=models.BooleanField(default=False),
        ),
    ]

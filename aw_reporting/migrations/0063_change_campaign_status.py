# Generated by Django 2.2.4 on 2019-10-16 18:05

from django.db import migrations


def change_campaign_status_eligible_to_serving(apps, schema_editor):
    Campaign = apps.get_model("aw_reporting", "Campaign")
    Campaign.objects.filter(status="eligible").update(status="serving")


def reverse_code(apps, schema_editor):
    pass


class Migration(migrations.Migration):
    dependencies = [
        ("aw_reporting", "0062_add_device_tv_screens"),
    ]

    operations = [
        migrations.RunPython(change_campaign_status_eligible_to_serving, reverse_code)
    ]

# Generated by Django 2.2.4 on 2020-01-29 22:10

from django.db import migrations


def remove_demo(apps, schema_editor):
    account_model = apps.get_model("aw_reporting.Account")
    account_model.objects.filter(id="demo").delete()

    opportunity_model = apps.get_model("aw_reporting.Opportunity")
    opportunity_model.objects.filter(id="demo").delete()


class Migration(migrations.Migration):
    dependencies = [
        ("aw_reporting", "0081_auto_20200124_1817"),
    ]

    operations = [
        migrations.RunPython(remove_demo),
    ]

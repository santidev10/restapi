# Generated by Django 2.2.4 on 2019-10-07 15:47

from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ('ads_analyzer', '0003_auto_20191007_1420'),
    ]

    operations = [
        migrations.AlterField(
            model_name='opportunitytargetingreport',
            name='date_from',
            field=models.DateField(null=True),
        ),
        migrations.AlterField(
            model_name='opportunitytargetingreport',
            name='date_to',
            field=models.DateField(null=True),
        ),
    ]

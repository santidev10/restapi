# Generated by Django 3.1.6 on 2021-02-02 10:34

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('aw_reporting', '0104_ad_creative_tracking_url_template'),
    ]

    operations = [
        migrations.AlterField(
            model_name='campaignhistory',
            name='changes',
            field=models.JSONField(),
        ),
    ]

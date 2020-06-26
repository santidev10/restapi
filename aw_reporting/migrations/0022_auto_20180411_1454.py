# -*- coding: utf-8 -*-
# Generated by Django 1.9.9 on 2018-04-11 14:54
from __future__ import unicode_literals

import django.db.models.deletion
from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ("aw_reporting", "0021_update_campaign"),
    ]

    operations = [
        migrations.CreateModel(
            name="CampaignAgeRangeTargeting",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("age_range_id", models.SmallIntegerField()),
                ("campaign",
                 models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="age_range_targeting",
                                   to="aw_reporting.Campaign")),
            ],
        ),
        migrations.CreateModel(
            name="CampaignGenderTargeting",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("gender_id", models.SmallIntegerField()),
                ("campaign",
                 models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="gender_targeting",
                                   to="aw_reporting.Campaign")),
            ],
        ),
        migrations.CreateModel(
            name="CampaignLocationTargeting",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("campaign",
                 models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="location_targeting",
                                   to="aw_reporting.Campaign")),
                ("location",
                 models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to="aw_reporting.GeoTarget")),
            ],
        ),
        migrations.AlterUniqueTogether(
            name="campaignlocationtargeting",
            unique_together=set([("location", "campaign")]),
        ),
        migrations.AlterUniqueTogether(
            name="campaigngendertargeting",
            unique_together=set([("gender_id", "campaign")]),
        ),
        migrations.AlterUniqueTogether(
            name="campaignagerangetargeting",
            unique_together=set([("age_range_id", "campaign")]),
        ),
    ]

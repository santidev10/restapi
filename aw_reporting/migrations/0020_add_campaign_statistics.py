# -*- coding: utf-8 -*-
# Generated by Django 1.9.9 on 2018-04-04 16:15
from __future__ import unicode_literals

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('aw_reporting', '0019_add_salesforce'),
    ]

    operations = [
        migrations.CreateModel(
            name='CampaignStatistic',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('impressions', models.IntegerField(default=0)),
                ('video_views', models.IntegerField(default=0)),
                ('clicks', models.IntegerField(default=0)),
                ('cost', models.FloatField(default=0)),
                ('conversions', models.FloatField(default=0)),
                ('all_conversions', models.FloatField(default=0)),
                ('view_through', models.IntegerField(default=0)),
                ('video_views_25_quartile', models.FloatField(default=0)),
                ('video_views_50_quartile', models.FloatField(default=0)),
                ('video_views_75_quartile', models.FloatField(default=0)),
                ('video_views_100_quartile', models.FloatField(default=0)),
                ('date', models.DateField(db_index=True)),
                ('device_id', models.SmallIntegerField(db_index=True, default=0)),
                ('campaign', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='statistics', to='aw_reporting.Campaign')),
            ],
            options={
                'ordering': ['-date'],
            },
        ),
        migrations.AlterUniqueTogether(
            name='campaignstatistic',
            unique_together=set([('campaign', 'date', 'device_id')]),
        ),
    ]

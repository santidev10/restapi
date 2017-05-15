# -*- coding: utf-8 -*-
# Generated by Django 1.9.9 on 2017-05-15 11:42
from __future__ import unicode_literals

from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name='Account',
            fields=[
                ('id', models.CharField(max_length=15, primary_key=True, serialize=False)),
                ('name', models.CharField(max_length=250, null=True)),
                ('currency_code', models.CharField(max_length=5, null=True)),
                ('timezone', models.CharField(max_length=100, null=True)),
                ('can_manage_clients', models.BooleanField(default=False)),
                ('is_test_account', models.BooleanField(default=False)),
                ('visible', models.BooleanField(default=True)),
                ('manager', models.ForeignKey(null=True, on_delete=django.db.models.deletion.CASCADE, related_name='customers', to='aw_reporting.Account')),
            ],
        ),
        migrations.CreateModel(
            name='AccountConnection',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('refresh_token', models.CharField(max_length=100)),
                ('manager', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='connections', to='aw_reporting.Account')),
                ('user', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='account_connections', to=settings.AUTH_USER_MODEL)),
            ],
        ),
        migrations.CreateModel(
            name='AdGroup',
            fields=[
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
                ('id', models.CharField(max_length=15, primary_key=True, serialize=False)),
                ('name', models.CharField(max_length=250)),
                ('status', models.CharField(max_length=7, null=True)),
                ('cpv_bid', models.PositiveIntegerField(null=True)),
                ('cpm_bid', models.PositiveIntegerField(null=True)),
                ('cpc_bid', models.PositiveIntegerField(null=True)),
            ],
            options={
                'abstract': False,
            },
        ),
        migrations.CreateModel(
            name='Campaign',
            fields=[
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
                ('id', models.CharField(max_length=15, primary_key=True, serialize=False)),
                ('name', models.CharField(max_length=250)),
                ('start_date', models.DateField(db_index=True, null=True)),
                ('end_date', models.DateField(null=True)),
                ('type', models.CharField(max_length=20, null=True)),
                ('budget', models.FloatField(null=True)),
                ('status', models.CharField(max_length=7, null=True)),
                ('account', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='campaigns', to='aw_reporting.Account')),
            ],
            options={
                'abstract': False,
            },
        ),
        migrations.CreateModel(
            name='GeoTarget',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=100)),
                ('canonical_name', models.CharField(max_length=100)),
                ('country_code', models.CharField(max_length=2)),
                ('target_type', models.CharField(max_length=50)),
                ('status', models.CharField(max_length=10)),
                ('parent', models.ForeignKey(null=True, on_delete=django.db.models.deletion.CASCADE, to='aw_reporting.GeoTarget')),
            ],
        ),
        migrations.AddField(
            model_name='adgroup',
            name='campaign',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='aw_reporting.Campaign'),
        ),
    ]

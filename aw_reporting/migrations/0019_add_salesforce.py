# -*- coding: utf-8 -*-
# Generated by Django 1.9.9 on 2018-04-02 15:10
from __future__ import unicode_literals

import django.db.models.deletion
from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ("aw_reporting", "0018_auto_20180329_1513"),
    ]

    operations = [
        migrations.CreateModel(
            name="Activity",
            fields=[
                ("id", models.CharField(max_length=20, primary_key=True, serialize=False)),
                ("name", models.CharField(max_length=250)),
                ("type", models.CharField(db_index=True, max_length=10)),
                ("date", models.DateField()),
            ],
            options={
                "abstract": False,
            },
        ),
        migrations.CreateModel(
            name="Category",
            fields=[
                ("id", models.CharField(max_length=200, primary_key=True, serialize=False)),
            ],
            options={
                "abstract": False,
            },
        ),
        migrations.CreateModel(
            name="Contact",
            fields=[
                ("id", models.CharField(max_length=20, primary_key=True, serialize=False)),
                ("first_name", models.CharField(max_length=50, null=True)),
                ("last_name", models.CharField(max_length=50, null=True)),
            ],
            options={
                "abstract": False,
            },
        ),
        migrations.CreateModel(
            name="Flight",
            fields=[
                ("id", models.CharField(max_length=20, primary_key=True, serialize=False)),
                ("name", models.CharField(max_length=100)),
                ("start", models.DateField(null=True)),
                ("end", models.DateField(null=True)),
                ("month", models.SmallIntegerField(null=True)),
                ("cost", models.FloatField(null=True)),
                ("delivered", models.IntegerField(null=True)),
                ("ordered_cost", models.FloatField(null=True)),
                ("total_cost", models.FloatField(null=True)),
                ("ordered_units", models.IntegerField(null=True)),
            ],
            options={
                "ordering": ("start",),
            },
        ),
        migrations.CreateModel(
            name="OpPlacement",
            fields=[
                ("id", models.CharField(max_length=20, primary_key=True, serialize=False)),
                ("name", models.CharField(max_length=100)),
                ("goal_type_id", models.SmallIntegerField(null=True)),
                ("ordered_units", models.IntegerField(null=True)),
                ("ordered_rate", models.FloatField(null=True)),
                ("total_cost", models.FloatField(null=True)),
                ("start", models.DateField(null=True)),
                ("end", models.DateField(null=True)),
                ("number", models.CharField(max_length=10, null=True)),
                ("ad_words_placement", models.CharField(max_length=255, null=True)),
                ("placement_type", models.CharField(max_length=25, null=True)),
                ("dynamic_placement", models.CharField(max_length=25, null=True)),
                ("tech_fee", models.DecimalField(decimal_places=4, max_digits=12, null=True)),
                ("tech_fee_cap", models.DecimalField(decimal_places=4, max_digits=12, null=True)),
                ("tech_fee_type", models.CharField(choices=[("CPV", "CPV"), ("CPM", "CPM")], max_length=3, null=True)),
                ("max_allowed_bid", models.PositiveIntegerField(null=True)),
                ("automation_enabled", models.BooleanField(default=False)),
                ("incremental", models.BooleanField(default=False)),
            ],
            options={
                "ordering": ("name",),
            },
        ),
        migrations.CreateModel(
            name="Opportunity",
            fields=[
                ("id", models.CharField(max_length=20, primary_key=True, serialize=False)),
                ("aw_cid", models.CharField(max_length=60, null=True)),
                ("number", models.CharField(max_length=10, null=True)),
                ("name", models.CharField(db_index=True, max_length=250)),
                ("region_id", models.SmallIntegerField(null=True)),
                ("budget", models.FloatField(default=0)),
                ("io_start", models.DateField(null=True)),
                ("start", models.DateField(null=True)),
                ("end", models.DateField(null=True)),
                ("proposal_date", models.DateField(null=True)),
                ("goal_type_id", models.SmallIntegerField(default=0)),
                ("units", models.IntegerField(default=0)),
                ("video_views", models.IntegerField(null=True)),
                ("impressions", models.IntegerField(null=True)),
                ("cpv_cost", models.FloatField(null=True)),
                ("cpm_cost", models.FloatField(null=True)),
                ("cannot_roll_over", models.BooleanField(default=False)),
                ("stage", models.CharField(max_length=60, null=True)),
                ("probability", models.PositiveSmallIntegerField(null=True)),
                ("create_date", models.DateField(null=True)),
                ("close_date", models.DateField(null=True)),
                ("renewal_approved", models.BooleanField(default=False)),
                ("reason_for_close", models.TextField(default='')),
                ("ad_ops_email", models.EmailField(max_length=254, null=True)),
                ("am_email", models.EmailField(max_length=254, null=True)),
                ("sales_email", models.EmailField(max_length=254, null=True)),
                ("notes", models.TextField(blank=True, null=True)),
                ("brand", models.CharField(max_length=50, null=True)),
                ("iq_category_id", models.SmallIntegerField(null=True)),
                ("iq_region_id", models.SmallIntegerField(null=True)),
                ("contracted_cpm", models.FloatField(null=True)),
                ("contracted_cpv", models.FloatField(null=True)),
                ("demographic", models.CharField(default='', max_length=300)),
                ("geo_targeting", models.TextField(default='')),
                ("targeting_tactics", models.CharField(default='', max_length=400)),
                ("tags", models.CharField(default='', max_length=20)),
                ("types_of_targeting", models.CharField(default='', max_length=100)),
                ("apex_deal", models.BooleanField(default=False)),
                ("bill_of_third_party_numbers", models.BooleanField(default=False)),
            ],
            options={
                "ordering": ("-start",),
            },
        ),
        migrations.CreateModel(
            name="SFAccount",
            fields=[
                ("id", models.CharField(max_length=20, primary_key=True, serialize=False)),
                ("name", models.CharField(max_length=200)),
                ("parent", models.ForeignKey(null=True, on_delete=django.db.models.deletion.CASCADE,
                                             to="aw_reporting.SFAccount")),
            ],
            options={
                "abstract": False,
            },
        ),
        migrations.CreateModel(
            name="User",
            fields=[
                ("id", models.CharField(max_length=20, primary_key=True, serialize=False)),
                ("name", models.CharField(db_index=True, max_length=60)),
                ("photo_id", models.CharField(max_length=50, null=True)),
                ("email", models.EmailField(max_length=254, null=True)),
                ("is_active", models.BooleanField(default=False)),
            ],
            options={
                "abstract": False,
            },
        ),
        migrations.CreateModel(
            name="UserRole",
            fields=[
                ("id", models.CharField(max_length=20, primary_key=True, serialize=False)),
                ("name", models.CharField(max_length=200)),
            ],
            options={
                "abstract": False,
            },
        ),
        migrations.AddField(
            model_name="user",
            name="role",
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.CASCADE, related_name="users",
                                    to="aw_reporting.UserRole"),
        ),
        migrations.AddField(
            model_name="opportunity",
            name="account",
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.CASCADE,
                                    to="aw_reporting.SFAccount"),
        ),
        migrations.AddField(
            model_name="opportunity",
            name="account_manager",
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL,
                                    related_name="managed_opportunities", to="aw_reporting.User"),
        ),
        migrations.AddField(
            model_name="opportunity",
            name="ad_ops_manager",
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL,
                                    related_name="ad_managed_opportunities", to="aw_reporting.User"),
        ),
        migrations.AddField(
            model_name="opportunity",
            name="ad_ops_qa_manager",
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL,
                                    related_name="qa_managed_opportunities", to="aw_reporting.User"),
        ),
        migrations.AddField(
            model_name="opportunity",
            name="agency",
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.CASCADE, to="aw_reporting.Contact"),
        ),
        migrations.AddField(
            model_name="opportunity",
            name="category",
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL,
                                    related_name="opportunities", to="aw_reporting.Category"),
        ),
        migrations.AddField(
            model_name="opportunity",
            name="sales_manager",
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL,
                                    related_name="sold_opportunities", to="aw_reporting.User"),
        ),
        migrations.AddField(
            model_name="opplacement",
            name="opportunity",
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="placements",
                                    to="aw_reporting.Opportunity"),
        ),
        migrations.AddField(
            model_name="flight",
            name="placement",
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="flights",
                                    to="aw_reporting.OpPlacement"),
        ),
        migrations.AddField(
            model_name="activity",
            name="account",
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.CASCADE, related_name="activities",
                                    to="aw_reporting.SFAccount"),
        ),
        migrations.AddField(
            model_name="activity",
            name="opportunity",
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.CASCADE, related_name="activities",
                                    to="aw_reporting.Opportunity"),
        ),
        migrations.AddField(
            model_name="activity",
            name="owner",
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="activities",
                                    to="aw_reporting.User"),
        ),
        migrations.AddField(
            model_name="campaign",
            name="salesforce_placement",
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL,
                                    related_name="adwords_campaigns", to="aw_reporting.OpPlacement"),
        ),
    ]

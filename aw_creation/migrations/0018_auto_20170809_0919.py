# -*- coding: utf-8 -*-
# Generated by Django 1.9.9 on 2017-08-09 09:19
from __future__ import unicode_literals

import django.core.validators
import django.db.models.deletion
from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ('aw_creation', '0017_auto_20170728_0938'),
    ]

    operations = [
        migrations.CreateModel(
            name='AccountOptimizationSetting',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('average_cpv', models.DecimalField(blank=True, decimal_places=3, max_digits=6, null=True)),
                ('average_cpm', models.DecimalField(blank=True, decimal_places=3, max_digits=6, null=True)),
                ('video_view_rate', models.DecimalField(blank=True, decimal_places=3, max_digits=6, null=True)),
                ('ctr', models.DecimalField(blank=True, decimal_places=3, max_digits=6, null=True)),
                ('ctr_v', models.DecimalField(blank=True, decimal_places=3, max_digits=6, null=True)),
                ('item',
                 models.OneToOneField(on_delete=django.db.models.deletion.CASCADE, related_name='optimization_setting',
                                      to='aw_creation.AccountCreation')),
            ],
            options={
                'abstract': False,
            },
        ),
        migrations.CreateModel(
            name='CampaignOptimizationSetting',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('average_cpv', models.DecimalField(blank=True, decimal_places=3, max_digits=6, null=True)),
                ('average_cpm', models.DecimalField(blank=True, decimal_places=3, max_digits=6, null=True)),
                ('video_view_rate', models.DecimalField(blank=True, decimal_places=3, max_digits=6, null=True)),
                ('ctr', models.DecimalField(blank=True, decimal_places=3, max_digits=6, null=True)),
                ('ctr_v', models.DecimalField(blank=True, decimal_places=3, max_digits=6, null=True)),
            ],
            options={
                'abstract': False,
            },
        ),
        migrations.AlterUniqueTogether(
            name='adgroupoptimizationtuning',
            unique_together=set([]),
        ),
        migrations.RemoveField(
            model_name='adgroupoptimizationtuning',
            name='item',
        ),
        migrations.AlterUniqueTogether(
            name='campaignoptimizationtuning',
            unique_together=set([]),
        ),
        migrations.RemoveField(
            model_name='campaignoptimizationtuning',
            name='item',
        ),
        migrations.AlterField(
            model_name='adcreation',
            name='video_url',
            field=models.URLField(default='', validators=[django.core.validators.RegexValidator(
                '^(?:https?:/{2})?(?:w{3}\\.)?youtu(?:be)?\\.(?:com|be)(?:/watch\\?v=|/video/|/)([^\\s&\\?]+)$',
                'Wrong video url')]),
        ),
        migrations.AlterField(
            model_name='adgroupcreation',
            name='ad_group',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE,
                                    related_name='ad_group_creation', to='aw_reporting.AdGroup'),
        ),
        migrations.AlterField(
            model_name='campaigncreation',
            name='campaign',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE,
                                    related_name='campaign_creation', to='aw_reporting.Campaign'),
        ),
        migrations.DeleteModel(
            name='AdGroupOptimizationTuning',
        ),
        migrations.DeleteModel(
            name='CampaignOptimizationTuning',
        ),
        migrations.AddField(
            model_name='campaignoptimizationsetting',
            name='item',
            field=models.OneToOneField(on_delete=django.db.models.deletion.CASCADE,
                                       related_name='optimization_setting', to='aw_creation.CampaignCreation'),
        ),
    ]

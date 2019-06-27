# -*- coding: utf-8 -*-
# Generated by Django 1.9.9 on 2018-04-13 11:48
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('aw_reporting', '0022_auto_20180411_1454'),
    ]

    operations = [
        migrations.AddField(
            model_name='campaign',
            name='targeting_channels',
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name='campaign',
            name='targeting_custom_affinity',
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name='campaign',
            name='targeting_excluded_channels',
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name='campaign',
            name='targeting_excluded_keywords',
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name='campaign',
            name='targeting_excluded_topics',
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name='campaign',
            name='targeting_interests',
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name='campaign',
            name='targeting_keywords',
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name='campaign',
            name='targeting_remarketings',
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name='campaign',
            name='targeting_topics',
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name='campaign',
            name='targeting_videos',
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name='campaign',
            name='tracking_template_is_set',
            field=models.BooleanField(default=False),
        ),
    ]
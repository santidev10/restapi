# -*- coding: utf-8 -*-
# Generated by Django 1.9.9 on 2017-08-30 13:41
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('aw_creation', '0019_merge'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='campaigncreation',
            name='age_ranges_raw',
        ),
        migrations.RemoveField(
            model_name='campaigncreation',
            name='genders_raw',
        ),
        migrations.RemoveField(
            model_name='campaigncreation',
            name='parents_raw',
        ),
        migrations.AlterField(
            model_name='adgroupcreation',
            name='age_ranges_raw',
            field=models.CharField(default='["AGE_RANGE_18_24", "AGE_RANGE_25_34", "AGE_RANGE_35_44", "AGE_RANGE_45_54", "AGE_RANGE_55_64", "AGE_RANGE_65_UP", "AGE_RANGE_UNDETERMINED"]', max_length=200),
        ),
        migrations.AlterField(
            model_name='adgroupcreation',
            name='genders_raw',
            field=models.CharField(default='["GENDER_FEMALE", "GENDER_MALE", "GENDER_UNDETERMINED"]', max_length=100),
        ),
        migrations.AlterField(
            model_name='adgroupcreation',
            name='parents_raw',
            field=models.CharField(default='["PARENT_PARENT", "PARENT_NOT_A_PARENT", "PARENT_UNDETERMINED"]', max_length=100),
        ),
    ]

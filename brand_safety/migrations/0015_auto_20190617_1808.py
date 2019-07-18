# -*- coding: utf-8 -*-
# Generated by Django 1.11.13 on 2019-06-17 18:08
from __future__ import unicode_literals

from django.db import migrations, models
import django.utils.timezone


class Migration(migrations.Migration):

    dependencies = [
        ('brand_safety', '0014_badwordhistory'),
    ]

    operations = [
        migrations.RenameField(
            model_name='badwordhistory',
            old_name='badword',
            new_name='tag',
        ),
        migrations.AddField(
            model_name='badwordhistory',
            name='action',
            field=models.CharField(db_column='action', default='N/A', max_length=30),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name='badwordhistory',
            name='created_at',
            field=models.DateTimeField(auto_now_add=True, db_column='created_at', default=django.utils.timezone.now),
            preserve_default=False,
        ),
        migrations.AlterField(
            model_name='badwordhistory',
            name='id',
            field=models.AutoField(db_column='id', primary_key=True, serialize=False),
        ),
    ]

# -*- coding: utf-8 -*-
# Generated by Django 1.11.13 on 2019-05-29 00:01
from __future__ import unicode_literals

import django.utils.timezone
from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ('audit_tool', '0023_auditchannelmeta_video_count'),
        ('brand_safety', '0011_auto_20190520_1655'),
    ]

    operations = [
        migrations.AddField(
            model_name='badword',
            name='created_at',
            field=models.DateTimeField(auto_now_add=True, default=django.utils.timezone.now),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name='badword',
            name='deleted_at',
            field=models.DateTimeField(db_index=True, default=None, null=True),
        ),
        migrations.AddField(
            model_name='badword',
            name='language',
            field=models.ForeignKey(default=None, null=True, on_delete=django.db.models.deletion.CASCADE,
                                    related_name='bad_words', to='audit_tool.AuditLanguage'),
        ),
        migrations.AlterField(
            model_name='badword',
            name='name',
            field=models.CharField(db_index=True, max_length=80),
        ),
        migrations.AlterUniqueTogether(
            name='badword',
            unique_together=set([('name', 'category', 'language')]),
        ),
    ]

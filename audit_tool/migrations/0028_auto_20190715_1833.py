# -*- coding: utf-8 -*-
# Generated by Django 1.11.13 on 2019-07-15 18:33
from __future__ import unicode_literals

import django.contrib.postgres.fields.jsonb
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('audit_tool', '0027_auditprocessor_name'),
    ]

    operations = [
        migrations.CreateModel(
            name='BlacklistItem',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('item_type', models.IntegerField(db_index=True)),
                ('item_id', models.CharField(db_index=True, max_length=64)),
                ('item_id_hash', models.BigIntegerField(db_index=True)),
                ('blacklist_category', django.contrib.postgres.fields.jsonb.JSONField(default={})),
            ],
        ),
        migrations.AlterUniqueTogether(
            name='blacklistitem',
            unique_together=set([('item_type', 'item_id')]),
        ),
    ]

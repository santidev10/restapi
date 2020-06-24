# -*- coding: utf-8 -*-
# Generated by Django 1.11.13 on 2019-04-10 18:16
from __future__ import unicode_literals

import django.contrib.postgres.fields.jsonb
import django.db.models.deletion
from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ('audit_tool', '0010_auto_20190312_2055'),
    ]

    operations = [
        migrations.CreateModel(
            name='AuditCategory',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('category', models.CharField(max_length=64, unique=True)),
            ],
        ),
        migrations.CreateModel(
            name='AuditChannel',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('channel_id', models.CharField(max_length=50, unique=True)),
                ('channel_id_hash', models.BigIntegerField(db_index=True, default=0)),
                ('processed', models.BooleanField(db_index=True, default=False)),
            ],
        ),
        migrations.CreateModel(
            name='AuditChannelMeta',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(default=None, max_length=255, null=True)),
                ('description', models.TextField(default=None)),
                ('keywords', models.TextField(default=None)),
                ('subscribers', models.BigIntegerField(db_index=True, default=0)),
                ('emoji', models.BooleanField(db_index=True, default=False)),
                ('channel',
                 models.OneToOneField(on_delete=django.db.models.deletion.CASCADE, to='audit_tool.AuditChannel')),
            ],
        ),
        migrations.CreateModel(
            name='AuditCountry',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('country', models.CharField(max_length=64, unique=True)),
            ],
        ),
        migrations.CreateModel(
            name='AuditLanguage',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('language', models.CharField(max_length=64, unique=True)),
            ],
        ),
        migrations.CreateModel(
            name='AuditProcessor',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('created', models.DateTimeField(auto_now_add=True, db_index=True)),
                ('updated', models.DateTimeField(default=None, null=True)),
                ('completed', models.DateTimeField(default=None, null=True)),
                ('max_recommended', models.IntegerField(default=100000)),
                ('params', django.contrib.postgres.fields.jsonb.JSONField(default={})),
            ],
        ),
        migrations.CreateModel(
            name='AuditVideo',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('video_id', models.CharField(max_length=50, unique=True)),
                ('video_id_hash', models.BigIntegerField(db_index=True, default=0)),
                ('channel', models.ForeignKey(default=None, null=True, on_delete=django.db.models.deletion.CASCADE,
                                              to='audit_tool.AuditChannel')),
            ],
        ),
        migrations.CreateModel(
            name='AuditVideoMeta',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(default=None, max_length=255, null=True)),
                ('description', models.TextField(default=None, null=True)),
                ('keywords', models.TextField(default=None, null=True)),
                ('views', models.BigIntegerField(db_index=True, default=0)),
                ('likes', models.BigIntegerField(db_index=True, default=0)),
                ('dislikes', models.BigIntegerField(db_index=True, default=0)),
                ('emoji', models.BooleanField(db_index=True, default=False)),
                ('publish_date', models.DateTimeField(db_index=True, default=None, null=True)),
                ('category', models.ForeignKey(default=None, null=True, on_delete=django.db.models.deletion.CASCADE,
                                               to='audit_tool.AuditCategory')),
                ('language', models.ForeignKey(default=None, null=True, on_delete=django.db.models.deletion.CASCADE,
                                               to='audit_tool.AuditLanguage')),
                ('video',
                 models.OneToOneField(on_delete=django.db.models.deletion.CASCADE, to='audit_tool.AuditVideo')),
            ],
        ),
        migrations.CreateModel(
            name='AuditVideoProcessor',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('processed', models.DateTimeField(db_index=True, default=None, null=True)),
                ('audit',
                 models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='audit_tool.AuditProcessor')),
                ('video', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='avp_video',
                                            to='audit_tool.AuditVideo')),
                ('video_source',
                 models.ForeignKey(default=None, null=True, on_delete=django.db.models.deletion.CASCADE,
                                   related_name='avp_video_source', to='audit_tool.AuditVideo')),
            ],
        ),
        migrations.AddField(
            model_name='auditchannelmeta',
            name='country',
            field=models.ForeignKey(default=None, null=True, on_delete=django.db.models.deletion.CASCADE,
                                    to='audit_tool.AuditCountry'),
        ),
        migrations.AddField(
            model_name='auditchannelmeta',
            name='language',
            field=models.ForeignKey(default=None, null=True, on_delete=django.db.models.deletion.CASCADE,
                                    to='audit_tool.AuditLanguage'),
        ),
        migrations.AlterUniqueTogether(
            name='auditvideoprocessor',
            unique_together=set([('audit', 'video')]),
        ),
    ]

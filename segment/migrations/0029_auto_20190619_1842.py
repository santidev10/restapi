# -*- coding: utf-8 -*-
# Generated by Django 1.11.13 on 2019-06-19 18:42
from __future__ import unicode_literals

from django.conf import settings
import django.contrib.postgres.fields.jsonb
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('contenttypes', '0002_remove_content_type_name'),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ('segment', '0028_persistentsegmentfileupload'),
    ]

    operations = [
        migrations.CreateModel(
            name='CustomSegmentFileUpload',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('completed_at', models.DateTimeField(db_index=True, default=None, null=True)),
                ('created_at', models.DateTimeField(auto_now_add=True, db_index=True)),
                ('download_url', models.TextField(null=True)),
                ('list_type', models.CharField(choices=[('blacklist', 'blacklist'), ('whitelist', 'whitelist')], max_length=10)),
                ('query', django.contrib.postgres.fields.jsonb.JSONField()),
                ('updated_at', models.DateTimeField(null=True)),
                ('segment_id', models.PositiveIntegerField(null=True)),
                ('content_type', models.ForeignKey(null=True, on_delete=django.db.models.deletion.CASCADE, to='contenttypes.ContentType')),
                ('owner', models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, to=settings.AUTH_USER_MODEL)),
            ],
        ),
        migrations.AlterUniqueTogether(
            name='customsegmentfileupload',
            unique_together=set([('content_type', 'segment_id')]),
        ),
    ]

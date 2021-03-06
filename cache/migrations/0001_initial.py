# Generated by Django 2.2.4 on 2019-11-15 20:47

import django.contrib.postgres.fields.jsonb
from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='CacheItem',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('key', models.CharField(max_length=255, unique=True)),
                ('value', django.contrib.postgres.fields.jsonb.JSONField(default=dict)),
            ],
            options={
                'abstract': False,
            },
        ),
    ]

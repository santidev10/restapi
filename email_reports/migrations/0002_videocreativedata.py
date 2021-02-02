# Generated by Django 3.0.4 on 2021-01-27 00:40

import django.contrib.postgres.fields.jsonb
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('email_reports', '0001_initial'),
    ]

    operations = [
        migrations.CreateModel(
            name='VideoCreativeData',
            fields=[
                ('id', models.CharField(max_length=50, primary_key=True, serialize=False, unique=True)),
                ('data', django.contrib.postgres.fields.jsonb.JSONField(default=dict)),
            ],
        ),
    ]
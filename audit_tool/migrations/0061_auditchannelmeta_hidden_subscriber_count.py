# Generated by Django 3.0.4 on 2020-05-12 21:30

from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ('audit_tool', '0060_auditchannelmeta_synced_with_viewiq'),
    ]

    operations = [
        migrations.AddField(
            model_name='auditchannelmeta',
            name='hidden_subscriber_count',
            field=models.BooleanField(default=False),
        ),
    ]

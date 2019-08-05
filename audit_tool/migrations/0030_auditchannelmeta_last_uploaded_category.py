# -*- coding: utf-8 -*-
# Generated by Django 1.11.13 on 2019-07-22 17:36
from __future__ import unicode_literals

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('audit_tool', '0029_auditchannelmeta_last_uploaded_view_count'),
    ]

    operations = [
        migrations.AddField(
            model_name='auditchannelmeta',
            name='last_uploaded_category',
            field=models.ForeignKey(default=None, null=True, on_delete=django.db.models.deletion.CASCADE, to='audit_tool.AuditCategory'),
        ),
    ]
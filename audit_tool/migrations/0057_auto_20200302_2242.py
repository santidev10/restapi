# Generated by Django 2.2.4 on 2020-03-02 22:42

from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    dependencies = [
        ('audit_tool', '0056_auto_20200219_2058'),
    ]

    operations = [
        migrations.AlterField(
            model_name='auditprocessor',
            name='completed',
            field=models.DateTimeField(db_index=True, default=None, null=True),
        ),
    ]

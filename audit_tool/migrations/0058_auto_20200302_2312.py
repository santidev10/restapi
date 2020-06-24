# Generated by Django 2.2.4 on 2020-03-02 23:12

from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ('audit_tool', '0057_auto_20200302_2242'),
    ]

    operations = [
        migrations.AlterIndexTogether(
            name='auditexporter',
            index_together={('audit', 'completed')},
        ),
        migrations.AlterIndexTogether(
            name='auditprocessor',
            index_together={('source', 'completed', 'audit_type')},
        ),
    ]

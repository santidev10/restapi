# Generated by Django 2.2.4 on 2019-11-15 18:40

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('audit_tool', '0042_auditchannel_processed_time'),
    ]

    operations = [
        migrations.AddField(
            model_name='auditvideoprocessor',
            name='channel',
            field=models.ForeignKey(default=None, null=True, on_delete=django.db.models.deletion.CASCADE, related_name='avp_audit_channel', to='audit_tool.AuditChannel'),
        ),
    ]
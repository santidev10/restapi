# Generated by Django 2.2.4 on 2019-11-19 23:54

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('audit_tool', '0043_auditvideoprocessor_channel'),
    ]

    operations = [
        migrations.AddField(
            model_name='auditexporter',
            name='export_as_videos',
            field=models.BooleanField(default=False),
        ),
    ]
# Generated by Django 2.2.4 on 2020-02-07 21:24

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('audit_tool', '0050_auditvideometa_made_for_kids'),
    ]

    operations = [
        migrations.AddField(
            model_name='auditexporter',
            name='machine',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='auditexporter',
            name='thread',
            field=models.IntegerField(null=True),
        ),
    ]

# Generated by Django 3.0.4 on 2020-04-02 17:23

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('audit_tool', '0059_auto_20200303_2358'),
    ]

    operations = [
        migrations.AddField(
            model_name='auditchannelmeta',
            name='synced_with_viewiq',
            field=models.NullBooleanField(db_index=True),
        ),
    ]

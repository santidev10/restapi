# Generated by Django 2.2.4 on 2020-01-15 18:31

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('audit_tool', '0047_remove_auditchannel_processed'),
    ]

    operations = [
        migrations.AddField(
            model_name='auditvideometa',
            name='age_restricted',
            field=models.NullBooleanField(db_index=True, default=None),
        ),
    ]
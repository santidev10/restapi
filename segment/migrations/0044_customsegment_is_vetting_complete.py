# Generated by Django 2.2.4 on 2020-02-25 21:23

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('segment', '0043_customsegment_audit_id'),
    ]

    operations = [
        migrations.AddField(
            model_name='customsegment',
            name='is_vetting_complete',
            field=models.BooleanField(db_index=True, default=False),
        ),
    ]
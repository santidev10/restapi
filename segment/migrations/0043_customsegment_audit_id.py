# Generated by Django 2.2.4 on 2020-02-08 04:40

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('segment', '0042_auto_20190916_0914'),
    ]

    operations = [
        migrations.AddField(
            model_name='customsegment',
            name='audit_id',
            field=models.IntegerField(db_index=True, default=None, null=True),
        ),
    ]

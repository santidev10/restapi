# Generated by Django 3.0.4 on 2020-08-24 19:28

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('aw_reporting', '0099_auto_20200819_1542'),
    ]

    operations = [
        migrations.AddField(
            model_name='account',
            name='active_view_viewability',
            field=models.FloatField(db_index=True, default=0),
        ),
    ]

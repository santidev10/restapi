# Generated by Django 3.0.4 on 2020-05-05 17:38

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('aw_reporting', '0087_auto_20200320_2339'),
    ]

    operations = [
        migrations.AddField(
            model_name='campaign',
            name='bidding_strategy_type',
            field=models.CharField(default=None, max_length=30, null=True),
        ),
    ]

# Generated by Django 3.1.6 on 2021-02-26 16:19

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('aw_reporting', '0106_auto_20210202_1034'),
    ]

    operations = [
        migrations.AddField(
            model_name='opportunity',
            name='config',
            field=models.JSONField(default=None, null=True),
        ),
    ]

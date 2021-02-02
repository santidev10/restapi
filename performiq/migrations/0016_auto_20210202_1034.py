# Generated by Django 3.1.6 on 2021-02-02 10:34

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('performiq', '0015_auto_20210106_1712'),
    ]

    operations = [
        migrations.AlterField(
            model_name='iqcampaign',
            name='params',
            field=models.JSONField(default=dict),
        ),
        migrations.AlterField(
            model_name='iqcampaign',
            name='results',
            field=models.JSONField(default=dict),
        ),
        migrations.AlterField(
            model_name='iqcampaignchannel',
            name='clean',
            field=models.BooleanField(db_index=True, default=None, null=True),
        ),
        migrations.AlterField(
            model_name='iqcampaignchannel',
            name='meta_data',
            field=models.JSONField(default=dict),
        ),
        migrations.AlterField(
            model_name='iqcampaignchannel',
            name='results',
            field=models.JSONField(default=dict),
        ),
    ]

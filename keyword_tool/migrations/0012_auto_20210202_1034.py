# Generated by Django 3.1.6 on 2021-02-02 10:34

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('keyword_tool', '0011_auto_20180705_1321'),
    ]

    operations = [
        migrations.AlterField(
            model_name='keywordslist',
            name='cum_average_volume',
            field=models.JSONField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='keywordslist',
            name='cum_average_volume_per_kw',
            field=models.JSONField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='keywordslist',
            name='top_keywords',
            field=models.JSONField(blank=True, null=True),
        ),
    ]

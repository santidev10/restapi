# Generated by Django 2.2.4 on 2020-01-30 18:11

from django.conf import settings
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('ads_analyzer', '0006_auto_20191010_0930'),
    ]

    operations = [
        migrations.AlterField(
            model_name='opportunitytargetingreport',
            name='recipients',
            field=models.ManyToManyField(related_name='opportunity_target_reports', to=settings.AUTH_USER_MODEL),
        ),
    ]
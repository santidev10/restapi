# Generated by Django 2.2.4 on 2019-09-26 08:14

import django.db.models.deletion
from django.conf import settings
from django.db import migrations
from django.db import models


class Migration(migrations.Migration):
    initial = True

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ('aw_reporting', '0059_flightstatistic'),
    ]

    operations = [
        migrations.CreateModel(
            name='OpportunityTargetingReport',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('date_from', models.DateField()),
                ('date_to', models.DateField()),
                ('external_link', models.URLField(default=None, null=True)),
                ('opportunity',
                 models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='aw_reporting.Opportunity')),
                ('recipients', models.ManyToManyField(to=settings.AUTH_USER_MODEL)),
            ],
        ),
        migrations.AddConstraint(
            model_name='opportunitytargetingreport',
            constraint=models.UniqueConstraint(fields=('opportunity', 'date_from', 'date_to'),
                                               name='unique_id_date_range'),
        ),
    ]

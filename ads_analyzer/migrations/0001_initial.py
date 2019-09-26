# Generated by Django 2.2.4 on 2019-09-26 12:57

from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
        ('aw_reporting', '0059_flightstatistic'),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name='OpportunityTargetingReport',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('date_from', models.DateField()),
                ('date_to', models.DateField()),
                ('s3_file_key', models.CharField(default=None, max_length=128, null=True)),
                ('status', models.CharField(choices=[('IN_PROGRESS', 'in_progress'), ('SUCCESS', 'success'), ('FAILED', 'failed')], default='in_progress', max_length=32)),
                ('opportunity', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='aw_reporting.Opportunity')),
                ('recipients', models.ManyToManyField(to=settings.AUTH_USER_MODEL)),
            ],
        ),
        migrations.AddConstraint(
            model_name='opportunitytargetingreport',
            constraint=models.UniqueConstraint(fields=('opportunity', 'date_from', 'date_to'), name='unique_id_date_range'),
        ),
    ]

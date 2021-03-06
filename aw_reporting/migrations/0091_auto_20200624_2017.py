# Generated by Django 3.0.4 on 2020-06-24 20:17

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('aw_reporting', '0090_opportunity_ias_campaign_name'),
    ]

    operations = [
        migrations.CreateModel(
            name='FlightPacingAllocation',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('date', models.DateField(db_index=True)),
                ('allocation', models.FloatField()),
                ('flight', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='allocations', to='aw_reporting.Flight')),
            ],
        ),
        migrations.AddConstraint(
            model_name='flightpacingallocation',
            constraint=models.UniqueConstraint(fields=('flight', 'date'), name='unique_goal'),
        ),
    ]

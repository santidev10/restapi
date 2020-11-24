# Generated by Django 3.0.4 on 2020-11-04 20:50

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('performiq', '0007_dv360advertiser_oauth_accounts'),
    ]

    operations = [
        migrations.AddField(
            model_name='campaign',
            name='advertiser',
            field=models.ForeignKey(default=None, null=True, on_delete=django.db.models.deletion.CASCADE, related_name='campaigns', to='performiq.DV360Advertiser'),
        ),
        migrations.AddField(
            model_name='campaign',
            name='display_name',
            field=models.CharField(default='', max_length=250),
        ),
        migrations.AddField(
            model_name='campaign',
            name='entity_status',
            field=models.SmallIntegerField(choices=[(0, 'Unspecified'), (1, 'Active'), (2, 'Archived'), (3, 'Draft'), (4, 'Paused'), (5, 'Scheduled for Deletion')], db_index=True, default=None, null=True),
        ),
        migrations.AddField(
            model_name='campaign',
            name='update_time',
            field=models.DateTimeField(default=None, null=True),
        ),
        migrations.AlterField(
            model_name='campaign',
            name='account',
            field=models.ForeignKey(default=None, null=True, on_delete=django.db.models.deletion.CASCADE, to='performiq.OAuthAccount'),
        ),
        migrations.AlterField(
            model_name='dv360advertiser',
            name='display_name',
            field=models.CharField(default='', max_length=250),
        ),
        migrations.AlterField(
            model_name='dv360advertiser',
            name='entity_status',
            field=models.SmallIntegerField(choices=[(0, 'Unspecified'), (1, 'Active'), (2, 'Archived'), (3, 'Draft'), (4, 'Paused'), (5, 'Scheduled for Deletion')], db_index=True, default=None, null=True),
        ),
        migrations.AlterField(
            model_name='dv360advertiser',
            name='update_time',
            field=models.DateTimeField(default=None, null=True),
        ),
        migrations.AlterField(
            model_name='dv360partner',
            name='display_name',
            field=models.CharField(default='', max_length=250),
        ),
        migrations.AlterField(
            model_name='dv360partner',
            name='entity_status',
            field=models.SmallIntegerField(choices=[(0, 'Unspecified'), (1, 'Active'), (2, 'Archived'), (3, 'Draft'), (4, 'Paused'), (5, 'Scheduled for Deletion')], db_index=True, default=None, null=True),
        ),
        migrations.AlterField(
            model_name='dv360partner',
            name='update_time',
            field=models.DateTimeField(default=None, null=True),
        ),
        migrations.DeleteModel(
            name='DV360Campaign',
        ),
    ]

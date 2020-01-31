# Generated by Django 2.2.4 on 2020-01-30 15:21

from django.db import migrations
from django.db import models
from django.db.models import BigIntegerField
from django.db.models.functions import Cast

FOREIGN_KEYS = [
    ('accountcreation', 'account'),
    ('adcreation', 'ad'),
    ('adgroupcreation', 'ad_group'),
    ('campaigncreation', 'campaign'),
]


def copy_fks(apps, schema_editor):
    for model_name, field_name in FOREIGN_KEYS:
        model = apps.get_model(f'aw_creation.{model_name}')
        model.objects.update(**{f'new_{field_name}': Cast(field_name, output_field=BigIntegerField())})

class Migration(migrations.Migration):
    dependencies = [
        ('aw_reporting', '0084_alter_account_manager'),
        ('aw_creation', '0043_auto_20190304_1501'),
    ]

    operations = [
        *[
            migrations.AddField(
                model_name=model,
                name=f'new_{column}',
                field=models.BigIntegerField(blank=True, null=True),
            ) for model, column in FOREIGN_KEYS
        ],
        migrations.RunPython(copy_fks),

        *[
            migrations.RemoveField(
                model_name=model,
                name=column,
            ) for model, column in FOREIGN_KEYS
        ],

        *[
            migrations.RenameField(
                model_name=model,
                old_name=f'new_{column}',
                new_name=column,
            ) for model, column in FOREIGN_KEYS
        ],

        migrations.AlterField(
            model_name='accountcreation',
            name='account',
            field=models.OneToOneField(blank=True, null=True, on_delete=models.deletion.CASCADE,
                                       related_name='account_creation', to='aw_reporting.Account'),
        ),
        migrations.AlterField(
            model_name='adcreation',
            name='ad',
            field=models.ForeignKey(blank=True, null=True, on_delete=models.deletion.CASCADE,
                                    related_name='ad_creation', to='aw_reporting.Ad'),
        ),
        migrations.AlterField(
            model_name='adgroupcreation',
            name='ad_group',
            field=models.ForeignKey(blank=True, null=True, on_delete=models.deletion.CASCADE,
                                    related_name='ad_group_creation', to='aw_reporting.AdGroup'),
        ),
        migrations.AddField(
            model_name='campaigncreation',
            name='campaign',
            field=models.ForeignKey(blank=True, null=True, on_delete=models.deletion.CASCADE,
                                    related_name='campaign_creation', to='aw_reporting.Campaign'),
        ),
    ]

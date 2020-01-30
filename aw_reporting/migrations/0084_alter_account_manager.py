# Generated by Django 2.2.4 on 2020-01-30 18:18

from django.db import migrations
from django.db import models


def copy_managers(apps, schema_editor):
    account_model = apps.get_model('aw_reporting.Account')
    account_manager_model = apps.get_model('aw_reporting.Account_Managers')
    for pair in account_manager_model.objects.all().values('from_account', 'to_account'):
        account = account_model.objects.get(id=int(pair['from_account']))
        account.new_managers.add(int(pair['to_account']))


def break_migration(*args, **kwargs):
    raise ValueError("break")


class Migration(migrations.Migration):
    dependencies = [
        ('aw_reporting', '0083_alter_pk_varchar_to_bigint'),
    ]

    operations = [
        migrations.AddField(
            model_name='account',
            name='new_managers',
            field=models.ManyToManyField(db_index=True, related_name='_account_new_managers_+',
                                         to='aw_reporting.Account'),
        ),
        migrations.RunPython(copy_managers),
        migrations.RemoveField(
            model_name='account',
            name='managers',
        ),
        migrations.RenameField(
            model_name='account',
            old_name='new_managers',
            new_name='managers',
        )
    ]

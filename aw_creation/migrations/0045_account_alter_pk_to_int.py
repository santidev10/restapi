# Generated by Django 2.2.4 on 2020-01-30 15:21

from django.db import migrations
from django.db import models


def assign_new_pk(apps, schema_editor):
    campaign_creation_model = apps.get_model('aw_creation.CampaignCreation')
    account_creation_model = apps.get_model('aw_creation.AccountCreation')
    for account in account_creation_model.objects.all():
        campaign_creation_model.objects.filter(account_creation=account.id).update(
            new_account_creation_id=account.new_id)


class Migration(migrations.Migration):
    dependencies = [
        ('aw_creation', '0044_alter_fk_to_aw_reporting'),
    ]

    operations = [

        migrations.AlterField(
            model_name='campaigncreation',
            name='account_creation',
            field=models.CharField(db_index=False, null=False, max_length=12)
        ),
        migrations.AlterField(
            model_name='accountcreation',
            name='id',
            field=models.CharField(db_index=False, null=False, max_length=12)
        ),

        migrations.AddField(
            model_name='accountcreation',
            name='new_id',
            field=models.AutoField(primary_key=True, serialize=False),
        ),
        migrations.AddField(
            model_name='campaigncreation',
            name='new_account_creation_id',
            field=models.IntegerField(serialize=False, null=True),
        ),
        migrations.RunPython(assign_new_pk),

        migrations.RemoveField(
            model_name='accountcreation',
            name='id',
        ),
        migrations.RemoveField(
            model_name='campaigncreation',
            name='account_creation',
        ),

        migrations.RenameField(
            model_name='accountcreation',
            old_name='new_id',
            new_name='id',
        ),
        migrations.RenameField(
            model_name='campaigncreation',
            old_name='new_account_creation_id',
            new_name='account_creation',
        ),

        migrations.AlterField(
            model_name='campaigncreation',
            name='account_creation',
            field=models.ForeignKey(on_delete=models.deletion.CASCADE,
                                    to='aw_creation.AccountCreation',
                                    related_name='campaign_creations',
                                    )
        ),
    ]

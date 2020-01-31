# Generated by Django 2.2.4 on 2020-01-30 18:18

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('aw_reporting', '0084_alter_account_manager'),
    ]

    operations = [
        migrations.AlterField(
            model_name='campaign',
            name='account',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.CASCADE, related_name='campaigns', to='aw_reporting.Account'),
        ),
    ]

# Generated by Django 3.0.4 on 2020-11-23 21:52

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('audit_tool', '0074_remove_iaschannel_history'),
    ]

    operations = [
        migrations.AddField(
            model_name='auditprocessor',
            name='machine',
            field=models.IntegerField(db_index=True, default=None, null=True),
        ),
        migrations.AddField(
            model_name='auditprocessor',
            name='seed_status',
            field=models.IntegerField(db_index=True, default=0),
        ),
        migrations.AddField(
            model_name='auditprocessor',
            name='thread',
            field=models.IntegerField(db_index=True, default=None, null=True),
        ),
        migrations.AlterField(
            model_name='auditcontentquality',
            name='id',
            field=models.IntegerField(choices=[(0, 'Low'), (1, 'Average'), (2, 'Premium')], primary_key=True, serialize=False),
        ),
    ]

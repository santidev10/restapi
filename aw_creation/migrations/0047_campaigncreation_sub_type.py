# Generated by Django 3.0.4 on 2020-05-20 20:53

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('aw_creation', '0046_adgroupcreation_status'),
    ]

    operations = [
        migrations.AddField(
            model_name='campaigncreation',
            name='sub_type',
            field=models.CharField(default=None, max_length=20, null=True),
        ),
    ]

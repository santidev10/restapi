# Generated by Django 2.2.4 on 2020-01-16 19:19

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('aw_reporting', '0073_auto_20200116_1918'),
    ]

    operations = [
        migrations.AlterField(
            model_name='opplacement',
            name='dynamic_placement',
            field=models.CharField(db_index=True, max_length=25, null=True),
        ),
    ]
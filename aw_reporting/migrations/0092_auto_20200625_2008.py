# Generated by Django 3.0.4 on 2020-06-25 20:08

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('aw_reporting', '0091_auto_20200624_2017'),
    ]

    operations = [
        migrations.CreateModel(
            name='Alert',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('record_id', models.CharField(db_index=True, max_length=20)),
                ('code', models.IntegerField(db_index=True)),
                ('message', models.TextField(null=True)),
            ],
        ),
        migrations.AddConstraint(
            model_name='alert',
            constraint=models.UniqueConstraint(fields=('record_id', 'code'), name='unique_alert'),
        ),
    ]

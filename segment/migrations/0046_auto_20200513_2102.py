# Generated by Django 3.0.4 on 2020-05-13 21:02

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('segment', '0045_customsegmentvettedfileupload'),
    ]

    operations = [
        migrations.AddField(
            model_name='customsegment',
            name='is_featured',
            field=models.BooleanField(db_index=True, default=False),
        ),
        migrations.AddField(
            model_name='customsegment',
            name='is_regenerating',
            field=models.BooleanField(db_index=True, default=False),
        ),
    ]
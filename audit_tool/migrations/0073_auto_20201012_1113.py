# Generated by Django 3.0.4 on 2020-10-12 11:13

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('audit_tool', '0072_auto_20200916_1835'),
    ]

    operations = [
        migrations.CreateModel(
            name='IASHistory',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('name', models.CharField(max_length=100)),
                ('started', models.DateTimeField(auto_now_add=True, db_index=True)),
                ('completed', models.DateTimeField(db_index=True, default=None, null=True)),
            ],
            options={
                'abstract': False,
            },
        ),
        migrations.AlterField(
            model_name='auditagegroup',
            name='id',
            field=models.IntegerField(choices=[(0, '0 - 3 Toddlers'), (1, '4 - 8 Young Kids'), (2, '9 - 12 Older Kids'), (3, '13 - 17 Teens'), (4, '18 - 35 Adults'), (5, '36 - 54 Older Adults'), (6, '55+ Seniors')], primary_key=True, serialize=False),
        ),
        migrations.AddField(
            model_name='iaschannel',
            name='history',
            field=models.ForeignKey(default=None, null=True, on_delete=django.db.models.deletion.CASCADE, to='audit_tool.IASHistory'),
        ),
    ]

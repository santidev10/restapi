# Generated by Django 3.1.6 on 2021-02-08 21:37

from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('userprofile', '0049_auto_20210202_1034'),
    ]

    operations = [
        migrations.CreateModel(
            name='Role',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=20, unique=True)),
                ('permissions', models.ManyToManyField(db_index=True, related_name='roles', to='userprofile.PermissionItem')),
            ],
        ),
        migrations.CreateModel(
            name='UserRole',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('role', models.OneToOneField(null=True, on_delete=django.db.models.deletion.SET_NULL, to='userprofile.role')),
                ('user', models.OneToOneField(on_delete=django.db.models.deletion.CASCADE, related_name='user_role', to=settings.AUTH_USER_MODEL)),
            ],
        ),
    ]

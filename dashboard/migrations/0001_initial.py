# Generated by Django 3.0.4 on 2020-07-29 23:22

from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name='OpportunityWatch',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('opportunity', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='aw_reporting.Opportunity')),
                ('user', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='watch', to=settings.AUTH_USER_MODEL)),
            ],
            options={
                'unique_together': {('opportunity', 'user')},
            },
        ),
    ]

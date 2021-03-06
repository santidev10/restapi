# Generated by Django 2.2.4 on 2020-03-20 22:04

from django.db import migrations
from userprofile.constants import DEFAULT_DOMAIN


def set_domain_fk(apps, schema_editor):
    UserProfile = apps.get_model("userprofile", "UserProfile")
    WhiteLabel = apps.get_model("userprofile", "WhiteLabel")

    apex_white_label, _ = WhiteLabel.objects.get_or_create(domain="apex", defaults=dict(config=dict(name="apex")))
    viewiq_white_label, _=  WhiteLabel.objects.get_or_create(domain=DEFAULT_DOMAIN, defaults=dict(config=dict(name=DEFAULT_DOMAIN)))

    UserProfile.objects.filter(email__icontains="apex").update(domain=apex_white_label)
    UserProfile.objects.exclude(email__icontains="apex").update(domain=viewiq_white_label)


def reverse(app, schema_editor):
    pass


class Migration(migrations.Migration):
    dependencies = [
        ('userprofile', '0046_userprofile_domain'),
    ]
    operations = [
        migrations.RunPython(set_domain_fk, reverse_code=reverse)
    ]

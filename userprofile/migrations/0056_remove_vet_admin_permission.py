# Generated by Django 3.1.6 on 2021-03-13 00:21

from django.db import migrations


BUILD__CTL_VET_ADMIN = "build.ctl_vet_admin"
BUILD__CTL_ANY_VETTING_STATUS = "build.ctl_any_vetting_status"
PERMS_TO_REMOVE = (BUILD__CTL_VET_ADMIN, BUILD__CTL_ANY_VETTING_STATUS)


def remove_old_permissions(apps, schema_editor):
    """
    remove the old permissions above
    :param apps:
    :param schema_editor:
    :return:
    """
    UserProfile = apps.get_model("userprofile", "UserProfile")
    for user in UserProfile.objects.all():
        should_save = False
        for perm in PERMS_TO_REMOVE:
            user_perm = user.perms.pop(perm, None)
            if user_perm is not None:
                should_save = True

        if should_save:
            user.save(update_fields=["perms"])


class Migration(migrations.Migration):

    dependencies = [
        ('userprofile', '0055_auto_20210306_0057'),
    ]

    operations = [
        migrations.RunPython(remove_old_permissions),
    ]
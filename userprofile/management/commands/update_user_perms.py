"""
Command to update user permissions for viewiq
"""
import logging
from django.core.management import BaseCommand

from userprofile.models import UserProfile

logger = logging.getLogger(__name__)
USER_DATA = (
    ("JohnSmithLT37@test.com", "enterprise"),
    ("JohnSmithLT13@test.com", "enterprise"),
    ("JohnSmithLT16@test.com", "enterprise"),
    ("anna.chumak1409@gmail.com", "free"),
    ("t+7@gmail.com", "free"),
    ("JohnSmithLT11@test.com", "enterprise"),
    ("martin21lutr@gmail.com", "free"),
    ("JohnSmithLT35@test.com", "enterprise"),
    ("JohnSmithLT48@test.com", "enterprise"),
    ("JohnSmithLT42@test.com", "enterprise"),
    ("JohnSmithLT23@test.com", "enterprise"),
    ("adamgatetest@gmail.com", "professional"),
    ("alexander.dobrzhansky@sigma.software", "free"),
    ("promopushmaster@gmail.com", "free"),
    ("actestsigma@gmail.com", "free"),
    ("JohnSmithLT47@test.com", "enterprise"),
    ("chf.sigmatestteam@gmail.com", "enterprise"),
    ("JohnSmithLT49@test.com", "enterprise"),
    ("test+5@test.com", "free"),
    ("t+6@gmail.com", "free"),
    ("JohnSmithLT46@test.com", "enterprise"),
    ("vladimir.makhonin@sigma.software", "free"),
    ("javascriptanywhere@gmail.com", "free"),
    ("magic_test@some.com", "free"),
    ("JohnSmithLT36@test.com", "enterprise"),
    ("handlingofficer1@gmail.com", "professional"),
    ("JohnSmithLT19@test.com", "enterprise"),
    ("JohnSmithLT17@test.com", "enterprise"),
    ("JohnSmithLT50@test.com", "enterprise"),
    ("JohnSmithLT43@test.com", "enterprise"),
    ("JohnSmithLT40@test.com", "enterprise"),
    ("JohnSmithLT38@test.com", "enterprise"),
    ("JohnSmithLT39@test.com", "enterprise"),
    ("makhoninv@gmail.com", "free"),
    ("JohnSmithLT44@test.com", "enterprise"),
    ("yevgen.tubaltsev@sigma.software", "free"),
    ("miroshnichenkoyuliazfort@gmail.com", "professional"),
    ("chfsigmaqa@gmail.com", "professional"),
    ("JohnSmithLT22@test.com", "enterprise"),
    ("dev.chfsigmatestteam@gmail.com", "free"),
    ("JohnSmithLT25@test.com", "enterprise"),
    ("user@mail22.com", "enterprise"),
    ("handlingofficer2@gmail.com", "professional"),
    ("alexander.bykov@channelfactory.com", "professional"),
    ("JohnSmithLT28@test.com", "enterprise"),
    ("123@gmail.com", "free"),
    ("mari.konareva@gmail.com", "professional"),
    ("testingsmth5@gmail.com", "free"),
    ("w@w.com", "free"),
    ("anna@test.com", "professional"),
    ("heorhii.pylypenko@simga.software", "enterprise"),
    ("grazian.moreno@channelfactory.com", "enterprise"),
    ("ymatso@gmail.com", "professional"),
    ("koz1024@yandex.ru", "free"),
    ("JohnSmithLT05@test.com", "enterprise"),
    ("JohnSmithLT01@test.com", "enterprise"),
    ("JohnSmithLT02@test.com", "enterprise"),
    ("JohnSmithLT06@test.com", "enterprise"),
    ("JohnSmithLT08@test.com", "enterprise"),
    ("JohnSmithLT09@test.com", "enterprise"),
    ("JohnSmithLT12@test.com", "enterprise"),
    ("JohnSmithLT14@test.com", "enterprise"),
    ("JohnSmithLT15@test.com", "enterprise"),
    ("JohnSmithLT21@test.com", "enterprise"),
    ("miroshnichenko.iuliia@gmail.com", "professional"),
    ("JohnSmithLT18@test.com", "enterprise"),
    ("JohnSmithLT20@test.com", "enterprise"),
    ("JohnSmithLT24@test.com", "enterprise"),
    ("JohnSmithLT26@test.com", "enterprise"),
    ("JohnSmithLT27@test.com", "enterprise"),
    ("JohnSmithLT30@test.com", "enterprise"),
    ("JohnSmithLT32@test.com", "enterprise"),
    ("JohnSmithLT33@test.com", "enterprise"),
    ("JohnSmithLT45@test.com", "enterprise"),
    ("sara.luckow@channelfactory.com", "enterprise"),
    ("alexmisnik91@gmail.com", "free"),
    ("ownera01@gmail.com", "professional"),
    ("edanzyyt@gmail.com", "professional"),
    ("ownera02@gmail.com", "professional"),
    ("qatest@test.com", "free"),
    ("a@a.com", "free"),
    ("saas@channelfactory.com", "enterprise"),
    ("tami.damiano@channelfactory.com", "enterprise"),
    ("rain.ua@gmail.com", "professional"),
    ("JohnSmithLT04@test.com", "enterprise"),
    ("t@t.com", "free"),
    ("JohnSmithLT03@test.com", "enterprise"),
    ("JohnSmithLT10@test.com", "enterprise"),
    ("tttt@t.com", "free"),
    ("JohnSmithLT07@test.com", "enterprise"),
    ("JohnSmithLT34@test.com", "enterprise"),
    ("JohnSmithLT41@test.com", "enterprise"),
    ("sdf@fsd.fsdf", "free"),
    ("JohnSmithLT29@test.com", "enterprise"),
    ("test@yahoo.com", "free"),
    ("JohnSmithLT31@test.com", "enterprise"),
    ("zhiltsovsi@gmail.com", "free"),
    ("aliona.lagno@gmail.com", "professional"),
    ("hal9000@channelfactory.com", "professional"),
    ("admin@admin.admin", "enterprise"),
    ("physicxx@gmail.com", "free"),
    ("molonationtv2@gmail.com", "professional"),
)
ACCESS_MAP = {
    'free': ['Highlights', ],
    'professional': ['Highlights', 'Discovery', 'Segments', 'Segments - pre-baked segments',
                     'Auth channels and audience data', ],
    'enterprise': ['Highlights', 'Discovery', 'Segments', 'Segments - pre-baked segments',
                   'Auth channels and audience data', ]
}


class Command(BaseCommand):
    def get_user(self, u_email):
        try:
            return UserProfile.objects.get(email=u_email)
        except UserProfile.DoesNotExist:
            return None

    def apply_perm_changes(self, user_email, plan_name):
        user = self.get_user(user_email)
        group_names = ACCESS_MAP.get(plan_name)
        if user and group_names:
            print(user.email)
            for group in group_names:
                user.add_custom_user_group(group)

    def handle(self, *args, **options):
        for (email, plan) in USER_DATA:
            self.apply_perm_changes(email, plan)

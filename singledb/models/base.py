from django.db import models

class Base(models.Model):
    inject_from = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.inject_from and hasattr(self, self.inject_from):
            related = getattr(self, self.inject_from)
            for field in related._meta.fields:
                if field.related_model == self.__class__:
                    continue
                name = field.name
                setattr(self, name, getattr(related, name))

                # next properties injection is used by ModelSerializer
                if not hasattr(self.__class__, name):
                    setattr(self.__class__, name, None)

    class Meta:
        abstract = True


class Timestampable(Base):
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True

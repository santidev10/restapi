from rest_framework.fields import ListField


class CharFieldListBased(ListField):
    def to_representation(self, data):
        values = super(CharFieldListBased, self).to_representation(data)
        return ",".join(values)

from types import SimpleNamespace


class MockGoogleAdsAPIResponse(object):
    def __init__(self):
        self.rows = []
        self.curr_row = SimpleNamespace()

    def set(self, resource, key, value, nested_key="value"):
        if getattr(self.curr_row, resource, None) is None:
            setattr(self.curr_row, resource, SimpleNamespace())
        resource_field = getattr(self.curr_row, resource)

        if "." in key:
            keys = key.split(".")
            for k in keys[:-1]:
                if getattr(resource_field, k, None) is None:
                    setattr(resource_field, k, SimpleNamespace())
                resource_field = getattr(resource_field, k)
            key = keys[-1]

        if nested_key:
            setattr(resource_field, key, SimpleNamespace())
            key_field = getattr(resource_field, key)
            setattr(key_field, nested_key, value)
        else:
            setattr(resource_field, key, value)

    def add_row(self):
        self.rows.append(self.curr_row)
        self.curr_row = SimpleNamespace()

    def get(self):
        return self.rows

    def __iter__(self):
        for row in self.rows:
            yield row

    def __getattribute__(self, name):
        try:
            attr = object.__getattribute__(self, name)
        except AttributeError:
            attr = None
        return attr
#
#
# class GoogleAdsRow(SimpleNamespace):
#     def __getattribute__(self, name):
#         try:
#             attr = object.__getattribute__(self, name)
#         except AttributeError:
#             attr = None
#         return attr
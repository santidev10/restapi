from django import template

register = template.Library()


@register.filter
def percentage(value, total=None):
    if value is None:
        return None

    if total:
        try:
            value = float(value)/float(total)
        except ZeroDivisionError:
            return None

    if value > 1:
        value = 1

    return "{value:.2%}".format(value=value or 0)

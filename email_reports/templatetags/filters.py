from django import template

register = template.Library()


@register.filter
def percentage(value, total=None):
    if total:
        try:
            value = float(value)/float(total)
        except ZeroDivisionError:
            return None
    return "{value:.2%}".format(value=value or 0)

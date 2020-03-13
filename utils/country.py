import pycountry


def get_country_by_code(country_code):
    country = None
    if country_code:
        country_obj = pycountry.countries.get(alpha_2=country_code) or pycountry.countries.get(alpha_3=country_code)
        if hasattr(country_obj, 'common_name'):
            country = country_obj.common_name
        elif hasattr(country_obj, 'name'):
            country = country_obj.name
    if country:
        country = country.replace(",", " -")
    return country


def get_country_code(country):
    country_obj = pycountry.countries.get(common_name=country) or pycountry.countries.get(name=country) or None
    country_code = country_obj.alpha_2 if country_obj else None
    return country_code

def move_us_to_top(countries, country_key="country"):
    """
    :param countries: list of countries from video or channel filter
    :param country_key: name of country name key
    :return: list of countries with US on top
    """
    for obj in countries:
        if obj.get(country_key) == "United States":
            i = countries.index(obj)
            countries = [countries[i]] + countries[:i] + countries[i + 1:]
    return countries

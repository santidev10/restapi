from math import floor

import brand_safety.constants as constants

def get_brand_safety_label(score):
    """
    Helper method to return appropriate brand safety score label
    :param score: Integer convertible value
    :return: str or None
    """
    try:
        score = int(score)
    except (ValueError, TypeError):
        return None

    if 90 <= score:
        label = constants.SAFE
    elif 80 <= score:
        label = constants.LOW_RISK
    elif 70 <= score:
        label = constants.RISKY
    else:
        label = constants.HIGH_RISK
    return label


def get_brand_safety_data(score):
    label = get_brand_safety_label(score)
    mapped_score = map_brand_safety_score(score)
    data = {
        "score": mapped_score,
        "label": label
    }
    return data


def map_brand_safety_score(score):
    """
    Map brand safety score of 0-100 to 0-10
    :param score: int
    :return: int
    """
    mapped = score
    if mapped is not None:
        try:
            mapped = floor(int(score) / 10)
        except ValueError:
            pass
    return mapped


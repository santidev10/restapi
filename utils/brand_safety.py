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

    # pylint: disable=misplaced-comparison-constant
    if 90 <= score:
        label = constants.SAFE
        score_threshold = 4
    elif 80 <= score:
        label = constants.LOW_RISK
        score_threshold = 3
    elif 70 <= score:
        label = constants.RISKY
        score_threshold = 2
    else:
        label = constants.HIGH_RISK
        score_threshold = 1
    return label, score_threshold
    # pylint: enable=misplaced-comparison-constant


def get_brand_safety_data(score):
    label = get_brand_safety_label(score)[0]
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


def map_score_threshold(score_threshold: int):
    """
    Map score threshold values to brand safety overall scores
    :param score_threshold: int
    :return: int
    """
    if score_threshold == 1:
        threshold = 0
    elif score_threshold == 2:
        threshold = 70
    elif score_threshold == 3:
        threshold = 80
    elif score_threshold == 4:
        threshold = 90
    else:
        threshold = None
    return threshold

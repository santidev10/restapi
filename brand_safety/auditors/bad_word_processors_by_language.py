from collections import defaultdict

from django.db.models import F
from flashtext import KeywordProcessor

from brand_safety.auditors.character_sets import (
    ARABIC_CHARACTER_SET_172,
    ARABIC_SUPPLEMENT_AND_EXTENDED_CHARACTER_SET_77,
    ARMENIAN_CHARACTER_SET_77,
    CYRILLIC_CHARCTER_SET_256,
    DEVANAGARI_CHARACTER_SET_128,
    ENGLISH_CHARACTERS_SET,
    GREEK_CHARACTER_SET_134,
    GREEK_EXTENDED_CHARACTER_SET_233,
    JAPANESE_HIRAGANA_CHARACTER_SET_77,
    JAPANESE_KATAKANA_CHARACTER_SET_80,
    TAGALOG_CHARACTER_SET_17,
    TAMIL_CHARACTER_SET_72,
    THAI_CHARACTER_SET_91,
    TURKISH_CHARACTER_SET_12,
    VIETMENESE_CHARACTER_SET_135,
    WESTERN_EUROPEAN_CHARACTER_SET_LOWERCASE,
)
from brand_safety.models import BadWord
from utils.utils import remove_tags_punctuation


def get_bad_word_processors_by_language() -> dict:
    """
    Generate dictionary of keyword processors by language
        Also provides an "all" key that contains every keyword
    :return: dict
    """
    bad_words_by_language = defaultdict(KeywordProcessor)
    all_words = BadWord.objects.annotate(language_name=F("language__language"))
    for word in all_words:
        language = word.language_name
        bad_words_by_language["all"].add_keyword(remove_tags_punctuation(word.name))
        bad_words_by_language[language].add_keyword(remove_tags_punctuation(word.name))
    # Cast back to dictionary to avoid creation of new keys
    bad_words_by_language = dict(bad_words_by_language)

    for language in bad_words_by_language:
        bad_words_by_language[language].set_non_word_boundaries(get_character_set(language))
    return bad_words_by_language


def get_character_set(language):
    characters = set(ENGLISH_CHARACTERS_SET)
    if language in ("all", ):
        characters |= ARABIC_CHARACTER_SET_172
        characters |= ARABIC_SUPPLEMENT_AND_EXTENDED_CHARACTER_SET_77
        characters |= ARMENIAN_CHARACTER_SET_77
        characters |= CYRILLIC_CHARCTER_SET_256
        characters |= DEVANAGARI_CHARACTER_SET_128
        characters |= GREEK_CHARACTER_SET_134
        characters |= GREEK_EXTENDED_CHARACTER_SET_233
        characters |= JAPANESE_HIRAGANA_CHARACTER_SET_77
        characters |= JAPANESE_KATAKANA_CHARACTER_SET_80
        characters |= TAGALOG_CHARACTER_SET_17
        characters |= TAMIL_CHARACTER_SET_72
        characters |= THAI_CHARACTER_SET_91
        characters |= TURKISH_CHARACTER_SET_12
        characters |= VIETMENESE_CHARACTER_SET_135
        characters |= WESTERN_EUROPEAN_CHARACTER_SET_LOWERCASE
    elif language in (
        "en", "cs", "da", "de", "es", "et", "fi", "fr", "hr", "hu", "it", "lt", "lv", "nl", "no",
        "pl", "pt", "ro", "sk", "sl", "sv",
    ):
        characters |= WESTERN_EUROPEAN_CHARACTER_SET_LOWERCASE
    elif language in ("ar", ):
        characters |= ARABIC_CHARACTER_SET_172
        characters |= ARABIC_SUPPLEMENT_AND_EXTENDED_CHARACTER_SET_77
    elif language in ("hy", ):
        characters |= ARMENIAN_CHARACTER_SET_77
    elif language in ("bg", "ru", "sr", "uk", ):
        characters |= CYRILLIC_CHARCTER_SET_256
    elif language in ("el", ):
        characters |= GREEK_CHARACTER_SET_134
        characters |= GREEK_EXTENDED_CHARACTER_SET_233
    elif language in ("hi", ):
        characters |= DEVANAGARI_CHARACTER_SET_128
    elif language in ("id", "ms", ):
        pass
    elif language in ("ja", ):
        characters |= JAPANESE_HIRAGANA_CHARACTER_SET_77
        characters |= JAPANESE_KATAKANA_CHARACTER_SET_80
        characters |= WESTERN_EUROPEAN_CHARACTER_SET_LOWERCASE
    elif language in ("ta", ):
        characters |= TAMIL_CHARACTER_SET_72
    elif language in ("th", ):
        characters |= THAI_CHARACTER_SET_91
    elif language in ("tl", ):
        characters |= TAGALOG_CHARACTER_SET_17
    elif language in ("tr", ):
        characters |= TURKISH_CHARACTER_SET_12
        characters |= WESTERN_EUROPEAN_CHARACTER_SET_LOWERCASE
    elif language in ("vi", ):
        characters |= VIETMENESE_CHARACTER_SET_135
    return characters

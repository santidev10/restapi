import re

from utils.utils import remove_tags_punctuation


class Trie():
    """Regexp::Trie in python. Creates a Trie out of a list of words. The trie can be exported to a Regexp pattern.
    The corresponding Regexp should match much faster than a simple Regexp union."""

    def __init__(self):
        self.data = {}

    def add(self, word):
        ref = self.data
        for char in word:
            ref[char] = char in ref and ref[char] or {}
            ref = ref[char]
        ref[''] = 1

    def dump(self):
        return self.data

    def quote(self, char):
        return re.escape(char)

    def _pattern(self, pData):
        data = pData
        if "" in data and len(data.keys()) == 1:
            return None

        alt = []
        cc = []
        q = 0
        for char in sorted(data.keys()):
            if isinstance(data[char], dict):
                try:
                    recurse = self._pattern(data[char])
                    alt.append(self.quote(char) + recurse)
                except:
                    cc.append(self.quote(char))
            else:
                q = 1
        cconly = not len(alt) > 0

        if len(cc) > 0:
            if len(cc) == 1:
                alt.append(cc[0])
            else:
                alt.append('[' + ''.join(cc) + ']')

        if len(alt) == 1:
            result = alt[0]
        else:
            result = "(?:" + "|".join(alt) + ")"

        if q:
            if cconly:
                result += "?"
            else:
                result = "(?:%s)?" % result
        return result

    def pattern(self):
        return self._pattern(self.dump())


# #Example on how to use the Trie class
# if __name__ == '__main__':
#     t = Trie()
#
#     for w in ['foobar', 'foobah', 'fooxar', 'foozap', 'fooza']:
#         t.add(w)
#     print(t.pattern())
#     #=> "foo(?:ba[hr]|xar|zap?)"

def get_optimized_regex(words_list=None, remove_tags_punctuation_from_words=False):
    """
    This function uses the Trie data structure above to generate an optimized regular expression.
    The output is either None or an optimized regular expression which is way faster than a normal words-union-regex.
    """

    result = None

    if isinstance(words_list, list) and len(words_list) > 0:
        keywords_trie = Trie()
        for w in words_list:
            if remove_tags_punctuation_from_words:
                w = remove_tags_punctuation(w)
            keywords_trie.add(w)
        result = re.compile(r"\b" + keywords_trie.pattern() + r"\b", re.IGNORECASE)

    return result

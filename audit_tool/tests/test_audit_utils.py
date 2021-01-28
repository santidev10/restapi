from django.test import TransactionTestCase
from audit_tool.utils.regex_trie import Trie


class GenerateAuditUtilsTestCase(TransactionTestCase):
    multi_db = True

    def test_regex_optimization_with_trie(self):
        """
        test that optimizing a regular expression using the trie data structure works as intended
        """
        words_list = ['foobar', 'foobah', 'fooxar', 'foozap', 'fooza']
        t = Trie()
        for w in words_list:
            t.add(w)
        self.assertEqual(t.pattern(), "foo(?:ba[hr]|xar|zap?)")

from django.test import testcases

from audit_tool import Keywords


class NetgativeKeywords(testcases.TestCase):
    def test_regexp_results(self):
        keywords = ('first', 'second', 'third', 'fourth')

        text = 'first word1 word2 first word3 first\n'\
               'word4 second second word5\n'\
               'word6 third *t*h*i*r*d* t h i r d t_h_i_r_d t-h-i-r-d word7'\
               '\nf\no\nu\nr\nt\nh\n'

        kw = Keywords()
        kw.compile_regexp(keywords)

        expected = (
            'first', 'first', 'first',
            'second', 'second',
            'third', 'third', 'third', 'third', 'third',
            'fourth',
        )

        result = kw.parse(text)
        self.assertTupleEqual(expected, result)

    def test_keywords_uniqueness(self):
        keywords = ('first', 'second', 'third', 'third', 'second')
        expected = ('first', 'second', 'third')
        unique_keywords = Keywords().unique(keywords)
        self.assertTupleEqual(expected, unique_keywords)

    def test_cleaning_keywords(self):
        keywords = (
            'word1 word2 w.o.r.d.3   w 0 R_d 4  w//o//r//d//5',
            'word1 word2 w.o.r.d.3   w 0 R_d 4  w//o//r//d//5',
            'word11 w!?o@r#d$1```52 w.o.r.d.13   w 0 R_d 14  w//o//r//d//15',
        )
        expected = (
            'word1word2word3w0rd4word5',
            'word1word2word3w0rd4word5',
            'word11word152word13w0rd14word15',
        )
        kw = Keywords()
        kw._keywords = keywords
        cleaned_keywords = kw.clean()
        self.assertTupleEqual(expected, cleaned_keywords)

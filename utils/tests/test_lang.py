from unittest import TestCase

from utils.lang import merge_sort


class MergeSort(TestCase):
    def test_empty(self):
        result_1 = list(merge_sort([]))
        result_2 = list(merge_sort([as_generator()]))
        result_3 = list(merge_sort([as_generator(), as_generator()]))

        self.assertEqual([], result_1)
        self.assertEqual([], result_2)
        self.assertEqual([], result_3)

    def test_simple(self):
        result_1 = list(merge_sort([as_generator(1), as_generator(2)]))
        result_2 = list(merge_sort([as_generator(2), as_generator(1)]))

        self.assertEqual([1, 2], result_1)
        self.assertEqual([1, 2], result_2)

    def test_long(self):
        result = merge_sort([
            as_generator(0, 1, 2, 3, 4, 6, 7, 8),
            as_generator(5)
        ])

        self.assertEqual(
            list(range(9)),
            list(result)
        )

    def test_list(self):
        result = list(merge_sort([[1], [2]]))

        self.assertEqual([1, 2], result)


def as_generator(*args):
    yield from args

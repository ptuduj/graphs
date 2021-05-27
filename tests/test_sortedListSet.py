from unittest import TestCase
from src.sets.set import SortedListSet


class TestSortedListSet(TestCase):
    def test_contains(self):
        s = SortedListSet([2, 3, 4, 5, 6])
        self.assertTrue(s.contains(2))
        self.assertFalse(s.contains(10))


    def test_difference(self):
        s1 = SortedListSet([2, 3, 4, 5, 6])
        s2 = SortedListSet([2, 3])
        self.assertEqual(s1.difference(s2), SortedListSet([4, 5, 6]))

        s1 = SortedListSet([2, 3])
        s2 = SortedListSet([2, 3])
        self.assertEqual(s1.difference(s2), SortedListSet([]))

        s1 = SortedListSet([2, 3, 5, 7])
        s2 = SortedListSet([8, 9])
        self.assertEqual(s1.difference(s2), s1)

        s1 = SortedListSet([2, 3, 5, 7])
        s2 = SortedListSet([4, 5, 9])
        self.assertEqual(s1.difference(s2), SortedListSet([2, 3, 7]))


    def test_intersect(self):
        s1 = SortedListSet([2, 3, 4, 5, 6])
        s2 = SortedListSet([2, 3])
        self.assertEqual(s1.intersect(s2), s2)

        s1 = SortedListSet([2, 3, 4, 5, 6])
        s2 = SortedListSet([2, 3, 6, 7])
        self.assertEqual(s1.intersect(s2), SortedListSet([2, 3, 6]))

        s1 = SortedListSet([2, 3, 4, 5, 6])
        s2 = SortedListSet([7])
        self.assertEqual(s1.intersect(s2), SortedListSet([]))

        s1 = SortedListSet([10, 4, 5, 6], from_sorted = False)
        s2 = SortedListSet([2, 6])
        self.assertEqual(s1.intersect(s2), SortedListSet([6]))

        s1 = SortedListSet([1, 3, 5])
        s2 = SortedListSet([2, 5])
        s3 = SortedListSet([2, 4])





    def test_union(self):
        s1 = SortedListSet([2, 3, 4, 5, 6])
        s2 = SortedListSet([2, 3, 4, 7, 8])
        self.assertEqual(s1.union(s2), SortedListSet([2, 3, 4, 5, 6, 7, 8]))

        s1 = SortedListSet([2, 3])
        s2 = SortedListSet([3])
        self.assertEqual(s1.union(s2), s1)

        s1 = SortedListSet([2, 3])
        s2 = SortedListSet([])
        self.assertEqual(s1.union(s2), s1)

        s1 = SortedListSet([10, 7, 2], from_sorted = False)
        s2 = SortedListSet([5, 10, 1], from_sorted = False)
        self.assertEqual(s1.union(s2), SortedListSet([1, 2, 5, 7, 10]))


    def test_add(self):
        s1 = SortedListSet([2, 3])
        self.assertEqual(s1.add(2), s1)

        s1 = SortedListSet([2, 3])
        self.assertEqual(s1.add(0), SortedListSet([0, 2, 3]))

        s2 = SortedListSet([2, 10, 3], from_sorted = False)
        self.assertEqual(s2.add(5), SortedListSet([2, 3, 5, 10]))


    def test_remove(self):
        s1 = SortedListSet([2, 4, 5])
        self.assertEqual(s1.remove(2), SortedListSet([4, 5]))
        self.assertEqual(s1.remove(4), SortedListSet([2, 5]))
        self.assertEqual(s1.remove(5), SortedListSet([2, 4]))
        self.assertEqual(s1.remove(3), s1)


    def test_elems_to(self):
        s1 = SortedListSet([2, 4, 5, 7])
        self.assertEqual(s1.elems_to(7), SortedListSet([2, 4, 5]))
        self.assertEqual(s1.elems_to(2), SortedListSet([]))
        self.assertEqual(s1.elems_to(4), SortedListSet([2]))
        with self.assertRaises(Exception):
            s1.elems_to(9)


    def test_elems_from(self):
        s1 = SortedListSet([2, 4, 5, 7])
        self.assertEqual(s1.elems_from(7), SortedListSet([7]))
        self.assertEqual(s1.elems_from(2), SortedListSet([2, 4, 5, 7]))
        self.assertEqual(s1.elems_from(4), SortedListSet([4, 5, 7]))
        with self.assertRaises(Exception):
            s1.elems_to(11)

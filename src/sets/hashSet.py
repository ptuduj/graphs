from __future__ import annotations
import bisect
from src.sets.set import Set


class HashSet(Set):
    def __init__(self, elems):
        self.hash_set = set(elems)

    def contains(self, elem) -> bool:
        return elem in self.hash_set

    def difference(self, other) -> Set:
        return HashSet(self.hash_set - other.hash_set)

    def intersect(self, other)-> Set:
        return HashSet(self.hash_set & other.hash_set)

    def union(self, other) -> Set:
        return HashSet(self.hash_set | other.hash_set)

    def add(self, elem)-> Set:
        return HashSet(self.hash_set | {elem})

    def remove(self, elem) -> Set:
        return HashSet(self.hash_set - {elem})

    def find(self, l, elem):
        index = bisect.bisect_left(l, elem)
        return index

    def elems_to(self, elem):
        if not self.contains(elem):
            raise Exception("elem not found")
        l = list(self.hash_set)
        l.index(elem)

        split_idx = l.index(elem)
        return HashSet(l[:split_idx])

    def elems_from(self, elem):
        if not self.contains(elem):
            raise Exception("elem not found")

        l = list(self.hash_set)
        l.index(elem)

        split_idx = l.index(elem)
        return HashSet(l[split_idx:])

    def __len__(self) -> int:
        return len(self.hash_set)

    def __iter__(self):
        return iter(self.hash_set)

    def __str__(self):
        return str(self.hash_set)

    def __eq__(self, other):
        return self.hash_set == other.hash_set


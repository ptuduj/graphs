from __future__ import annotations
import bisect
from src.sets.set import Set


class SortedListSet(Set):
    def __init__(self, l, from_sorted = True, sorting_lambda = None):
        self._sorted_list = l.copy()
        if not from_sorted:
            self._sorted_list.sort(key = sorting_lambda if sorting_lambda is not None else lambda x: x)

    def find(self,elem):
        index = bisect.bisect_left(self._sorted_list, elem)
        return index

    def _contains_with_index(self, elem,index):
        if index != len(self._sorted_list) and self._sorted_list[index] == elem:
            return True
        return False

    def contains(self, elem) -> bool:
        index = self.find(elem)
        return self._contains_with_index(elem,index)

    def elems_to(self, elem):
        if not self.contains(elem):
            raise Exception("elem not found")

        split_idx = self.find(elem)
        return SortedListSet(self._sorted_list[:split_idx])

    def elems_from(self,elem):
        if not self.contains(elem):
            raise Exception("elem not found")

        split_idx = self.find(elem)
        return SortedListSet(self._sorted_list[split_idx:])


    def difference(self, other) -> Set:
        res = []
        i, j = 0, 0
        len_this, len_other = len(self._sorted_list), len(other._sorted_list)

        while i < len_this and j < len_other:
            if self._sorted_list[i] == other._sorted_list[j]:
                i += 1
                j += 1
            elif self._sorted_list[i] < other._sorted_list[j]:
                res.append(self._sorted_list[i])
                i += 1
            else:
                j += 1
        return SortedListSet(res + self._sorted_list[i:])

    def intersect(self,other)-> Set:
        out = []
        sidx = 0
        oidx = 0
        while sidx < len(self._sorted_list) and oidx < len(other._sorted_list):
            diff = self._sorted_list[sidx] - other._sorted_list[oidx]
            if diff > 0:
                oidx+=1
            elif diff<0:
                sidx+=1
            else:
                out.append(self._sorted_list[sidx])
                oidx+=1
                sidx+=1
        return SortedListSet(out)

    def union(self, other) -> Set:
        res = []
        i, j = 0, 0
        len_this, len_other  = len(self._sorted_list), len(other._sorted_list)

        while i < len_this and j < len_other:
            if self._sorted_list[i] < other._sorted_list[j]:
                res.append(self._sorted_list[i])
                i += 1
            elif self._sorted_list[i] == other._sorted_list[j]:
                res.append(other._sorted_list[j])
                i += 1
                j += 1
            else:
                res.append(other._sorted_list[j])
                j += 1

        return SortedListSet(res + self._sorted_list[i:] + other._sorted_list[j:])

    def add(self, elem) -> Set:
        index = self.find(elem)
        if self.contains(elem):
            return self
        return SortedListSet(self._sorted_list[:index] + [elem] + self._sorted_list[index:])


    def remove(self, elem) -> Set:
        left_index = bisect.bisect_left(self._sorted_list, elem)
        right_index = bisect.bisect_right(self._sorted_list, elem)
        return SortedListSet(self._sorted_list[:left_index] + self._sorted_list[right_index:])

    def __len__(self) -> int:
        return len(self._sorted_list)

    def __iter__(self):
        return iter(self._sorted_list)

    def __str__(self):
        return str(self._sorted_list)

    def __eq__(self, other):
        return self._sorted_list == other._sorted_list


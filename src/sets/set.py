from __future__ import annotations
import bisect


class Set:
    def is_empty(self) -> bool:
        pass

    def difference(self, other) -> Set:
        pass

    def intersect(self,other) -> Set:
        pass

    def union(self,other) -> Set:
        pass

    def contains(self,elem) -> bool:
        pass

    def add(self,elem) -> Set:
        pass

    def remove(self,elem) -> Set:
        pass

    def __len__(self) -> int:
        pass

    def __iter__(self):
        pass

    def elems_to(self,elem) -> Set:
        pass

    def elems_from(self,elem) -> Set:
        pass

    @staticmethod
    def factory(t, elems):
        if isinstance(t, SortedListSet):
            return SortedListSet(elems)
        elif isinstance(t, HashSet):
            return HashSet(elems)


class HashSet(Set):
    def __init__(self, elems, from_sorted = True):
        self.hash_set = set(elems)

    def is_empty(self) -> bool:
        return len(self.hash_set) == 0

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
        if not isinstance(other, HashSet):
            return False
        return self.hash_set == other.hash_set

    def __hash__(self):
        return hash(tuple(self.hash_set))


class SortedListSet(Set):
    def __init__(self, l, from_sorted = True, sorting_lambda = None):
        self._sorted_list = l.copy()
        if not from_sorted:
            self._sorted_list.sort(key = sorting_lambda if sorting_lambda is not None else lambda x: x)

    def is_empty(self):
        return len(self._sorted_list) == 0

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
        if not isinstance(other, SortedListSet):
            return False
        return self._sorted_list == other._sorted_list

    def __hash__(self):
        return hash(tuple(self._sorted_list))

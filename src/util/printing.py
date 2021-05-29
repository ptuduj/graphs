
def print_in_n_chars(obj_,n):
    s = str(obj_)
    return s + ''.join([' ']* (n - len(s)))
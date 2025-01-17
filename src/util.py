from itertools import islice


def dict_slice(dictionary, start=0, stop=None) -> dict:
    return dict(islice(dictionary.items(), start, stop))


def discard_ones_digit(price: int) -> int:
    return price // 10 * 10

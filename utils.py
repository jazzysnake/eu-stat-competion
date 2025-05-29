import itertools


def batched(iterable, n, *, strict=False):
    """Yields successive n-sized chunks from an iterable.

    Args:
        iterable: The iterable to be batched.
        n: The size of each batch.
        strict: If True, raises ValueError if the last batch is not full.
                Defaults to False.

    Yields:
        tuple: A batch of items from the iterable.

    Raises:
        ValueError: If n is less than 1.
        ValueError: If `strict` is True and the last batch is incomplete.

    Example:
        >>> list(batched('ABCDEFG', 3))
        [('A', 'B', 'C'), ('D', 'E', 'F'), ('G',)]
        >>> list(batched('ABCDEFG', 3, strict=True))
        ValueError: batched(): incomplete batch
    """
    if n < 1:
        raise ValueError('n must be at least one')
    iterator = iter(iterable)
    while batch := tuple(itertools.islice(iterator, n)):
        if strict and len(batch) != n:
            raise ValueError('batched(): incomplete batch')
        yield batch

def howmany_within_range(row, minimum, maximum):
    count = 0
    for n in row:
        if minimum <= n <= maximum:
            count += 1
    return count


def howmany_within_range_rowonly(row, minimum=4, maximum=8):
    count = 0
    for n in row:
        if minimum <= n <= maximum:
            count += 1
    return count


def howmany_within_range2(i, row, minimum, maximum):
    count = 0
    for n in row:
        if minimum <= n <= maximum:
            count += 1
    return (i, count)

# Problem 1: Common items between two rows
def common_items(row_a, row_b):
    return sorted(list(set(row_a) & set(row_b)))


# Problem 3: Normalization function
def normalize(row):
    """
    Normalize a row to [0, 1] range using min-max normalization.
    Handle edge case where all values are the same.
    """
    min_val = min(row)
    max_val = max(row)

    # Handle edge case where all values are the same
    if max_val == min_val:
        return [0.0] * len(row)

    return [(x - min_val) / (max_val - min_val) for x in row]
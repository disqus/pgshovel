def distribute(consumers, resources):
    """
    Distributes a collection of resources for the provided consumers, returning
    a mapping of assigned resources for each consumer.

    This based on the Kafka rebalancing algorithm.
    """
    consumers = sorted(consumers)
    resources = sorted(resources)
    if not consumers:
        return {}

    per = len(resources) / len(consumers)
    rem = len(resources) % len(consumers)

    assignments = {}
    for i, consumer in enumerate(consumers):
        start = per * i + min(i, rem)
        count = per + (0 if (i + 1 > rem) else 1)
        assignments[consumer] = resources[start:start+count]

    return assignments

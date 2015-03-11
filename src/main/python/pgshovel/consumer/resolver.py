import json

from pgshovel.snapshot import (
    Snapshot,
    State,
    Transaction,
)


def create_simple_snapshot_builder(configuration, compact=True):
    """
    Creates and returns a function that transforms an event (caused by a
    mutation trigger) into a snapshot object.
    """
    assert not configuration.table.joins

    columns = set(configuration.table.columns)

    def extract_key(state):
        return state[configuration.table.primary_key]

    def reduce_state(state):
        return dict([item for item in state.items() if item[0] in columns])

    def get_transaction(payload):
        data = payload['transaction']
        return Transaction(data['id'], data['time'])

    def insert_snapshot(payload):
        state = payload['state']['new']
        return (
            Snapshot(
                key=extract_key(state),
                state=State(reduce_state(state)),
                transaction=get_transaction(payload),
            ),
        )

    def update_snapshot(payload):
        new_state = payload['state']['new']
        old_state = payload['state']['old']
        transaction = get_transaction(payload)
        snapshots = (
            Snapshot(
                key=extract_key(new_state),
                state=State(reduce_state(new_state)),
                transaction=transaction,
            ),
        )

        if extract_key(new_state) != extract_key(old_state):
            snapshots = snapshots + (
                Snapshot(
                    key=extract_key(old_state),
                    state=None,
                    transaction=transaction,
                ),
            )

        return snapshots

    def delete_snapshot(payload):
        return (
            Snapshot(
                key=extract_key(payload['state']['old']),
                state=None,
                transaction=get_transaction(payload),
            ),
        )

    handlers = {
        'INSERT': insert_snapshot,
        'UPDATE': update_snapshot,
        'DELETE': delete_snapshot,
    }

    def resolve(cursor, events):
        snapshots = []
        for event in events:
            payload = json.loads(event.data)
            handler = handlers[payload['operation']]
            snapshots.extend(handler(payload))
        return snapshots

    def resolve_compact(cursor, events):
        snapshots = {}
        for event in events:
            payload = json.loads(event.data)
            handler = handlers[payload['operation']]
            for snapshot in handler(payload):
                snapshots[snapshot.key] = snapshot
        return snapshots.values()

    # TODO: Enabling compaction should probably be a part of the group
    # configuration.
    return resolve_compact if compact else resolve

from cPickle import loads

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

    def get_transaction(event):
        return Transaction(*event.data.transaction)

    def insert_snapshot(event):
        state = event.data.states.new
        return (
            Snapshot(
                key=extract_key(state),
                state=State(reduce_state(state)),
                transaction=get_transaction(event),
            ),
        )

    def update_snapshot(event):
        states = event.data.states
        transaction = get_transaction(event)
        snapshots = (
            Snapshot(
                key=extract_key(states.new),
                state=State(reduce_state(states.new)),
                transaction=transaction,
            ),
        )

        if extract_key(states.new) != extract_key(states.old):
            snapshots = snapshots + (
                Snapshot(
                    key=extract_key(states.old),
                    state=None,
                    transaction=transaction,
                ),
            )

        return snapshots

    def delete_snapshot(event):
        state = event.data.states.old
        return (
            Snapshot(
                key=extract_key(state),
                state=None,
                transaction=get_transaction(event),
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
            handler = handlers[event.data.operation]
            snapshots.extend(handler(event))
        return snapshots

    def resolve_compact(cursor, events):
        snapshots = {}
        for event in events:
            handler = handlers[event.data.operation]
            for snapshot in handler(event):
                snapshots[snapshot.key] = snapshot
        return snapshots.values()

    # TODO: Enabling compaction should probably be a part of the group
    # configuration.
    return resolve_compact if compact else resolve

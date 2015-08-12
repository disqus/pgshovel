import uuid


class FormattedBatchIdentifier(object):
    def __init__(self, batch_identifier):
        self.batch_identifier = batch_identifier

    def __str__(self):
        return '{node.hex}/{id}'.format(
            node=uuid.UUID(bytes=self.batch_identifier.node),
            id=self.batch_identifier.id,
        )


class FormattedSnapshot(object):
    def __init__(self, snapshot, max=3):
        self.snapshot = snapshot
        self.max = max

    def __str__(self):
        active = self.snapshot.active
        return '{snapshot.min}:{snapshot.max}:[{active}{truncated}]'.format(
            snapshot=self.snapshot,
            active=','.join(map(str, active[:self.max])),
            truncated='' if len(active) <= self.max else ',+{0}...'.format(len(active) - self.max),
        )

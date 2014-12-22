CREATE FUNCTION ${schema}.capture()
RETURNS trigger
LANGUAGE plpythonu AS
$$TRIGGER$$
    json = SD.get('json')
    if not json:
        json = SD['json'] = __import__('json')

    statements = SD.get('statements')
    if not statements:
        statements = SD['statements'] = {
            'enqueue': plpy.prepare('SELECT pgq.insert_event($$1, $$2, $$3)', ["text", "text", "text"]),
            'transaction': plpy.prepare('SELECT txid_current() as id, extract(epoch from now()) as time'),
        }

    (group, alias) = TD['args']
    data = {
        'operation': TD['event'],
        'transaction': dict(plpy.execute(statements['transaction'])[0]),
        'group': group,
        'table': {
            'name': TD['table_name'],
            'alias': alias,
        },
        'state': {
            'new': TD['new'],
            'old': TD['old'],
        },
    }

    plpy.execute(statements['enqueue'], ('${queue}', 'operation', json.dumps(data)))
$$TRIGGER$$;

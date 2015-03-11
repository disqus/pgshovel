from collections import namedtuple

from pgshovel.interfaces.groups_pb2 import (
    JoinConfiguration,
    TableConfiguration,
)
from pgshovel.snapshot import (
    Column,
    Join,
    State,
    Table,
    build_result_expander,
    build_reverse_statements,
    build_statement,
    build_tree,
)


def test_build_tree():
    configuration = TableConfiguration(
        name='user',
        primary_key='id',
        columns=['username', 'email'],
        joins=[
            JoinConfiguration(
                table=TableConfiguration(
                    name='profile',
                    primary_key='id',
                    columns=['display_name', 'settings'],
                ),
                foreign_key='user_id',
            ),
        ],
    )

    tree = build_tree(configuration)

    user = Table(
        alias='user_0',
        name='user',
        primary_key='id',
        columns=['username', 'email'],
    )

    profile = Table(
        alias='profile_0',
        name='profile',
        primary_key='id',
        columns=['display_name', 'settings'],
    )

    user.joins.append(Join(Column(user, 'id'), Column(profile, 'user_id')))

    # XXX: Can't actually test simple equality because the presence of the
    # parent table in the Join causes a cycle and infinite recursion, so we
    # have to test around it.
    assert tree[:-1] == user[:-1]
    assert len(tree.joins) == 1
    assert tree.joins[0].left == Column(tree, 'id')
    assert tree.joins[0].right == user.joins[0].right


author = Table('a', 'author', 'id', ('name',))
book = Table('b', 'book', 'id', ('title', 'date',))
chapter = Table('c', 'chapter', 'id', ('title',))

author.joins.append(Join(Column(author, author.primary_key), Column(book, 'author_id')))
book.joins.append(Join(Column(book, book.primary_key), Column(chapter, 'book_id')))


def test_build_statement():
    assert build_statement(author) == \
        'SELECT ' \
            '"a"."id", "a"."name", ' \
            '"b"."id", "b"."title", "b"."date", ' \
            '"c"."id", "c"."title" ' \
        'FROM "author" AS "a" ' \
        'LEFT OUTER JOIN "book" AS "b" ON "a"."id" = "b"."author_id" ' \
        'LEFT OUTER JOIN "chapter" AS "c" ON "b"."id" = "c"."book_id" ' \
        'WHERE "a"."id" IN %s ' \
        'ORDER BY "a"."id" ASC, "b"."id" ASC, "c"."id" ASC'


def test_build_reverse_statements():
    assert build_reverse_statements(author) == {
        'book': [
            'SELECT DISTINCT "a"."id" AS "key" '
            'FROM "book" AS "b" '
            'INNER JOIN "author" AS "a" ON "b"."author_id" = "a"."id" '
            'WHERE "b"."id" IN %s'
        ],
        'chapter': [
            'SELECT DISTINCT "a"."id" AS "key" '
            'FROM "chapter" AS "c" '
            'INNER JOIN "book" AS "b" ON "c"."book_id" = "b"."id" '
            'INNER JOIN "author" AS "a" ON "b"."author_id" = "a"."id" '
            'WHERE "c"."id" IN %s'
        ],
    }


Author = namedtuple('Author', 'id name')
Book = namedtuple('Book', 'id title date')
Chapter = namedtuple('Chapter', 'id title')


def test_result_expander():
    expand = build_result_expander(author)

    results = (
        Author(1, 'ernest hemingway') + Book(1, 'the sun also rises', 1926) + Chapter(1, 'one'),
        Author(1, 'ernest hemingway') + Book(1, 'the sun also rises', 1926) + Chapter(2, 'two'),
        Author(1, 'ernest hemingway') + Book(1, 'the sun also rises', 1926) + Chapter(3, 'three'),
        Author(1, 'ernest hemingway') + Book(2, 'the old man and the sea', 1952) + Chapter(None, None),
        Author(1, 'ernest hemingway') + Book(None, None, None) + Chapter(None, None),
        Author(2, 'joseph heller') + Book(3, 'catch-22', 1961) + Chapter(None, None),
    )

    states = list(expand(results))

    key, state = states[0]
    assert key == 1
    assert state == State({'name': 'ernest hemingway'}, {
        'book.author_id': [
            State({'date': 1926, 'title': 'the sun also rises'}, {
                'chapter.book_id': [
                    State({'title': 'one'}),
                    State({'title': 'two'}),
                    State({'title': 'three'}),
                ],
            }),
            State({'date': 1952, 'title': 'the old man and the sea'}, {
                'chapter.book_id': [],
            }),
        ],
    })

    key, state = states[1]
    assert key == 2
    assert state == State({'name': 'joseph heller'}, {
        'book.author_id': [
            State({'date': 1961, 'title': 'catch-22'}, {
                'chapter.book_id': [],
            }),
        ],
    })

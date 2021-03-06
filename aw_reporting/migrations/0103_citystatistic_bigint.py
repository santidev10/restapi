# Generated by Django 3.0.4 on 2020-12-29 18:44

from django.db import migrations


def change_id_to_bigint(apps, schema_editor):
    # Remove atomicity to be able to save progress of migration if timeout occurs
    schema_editor.atomic.__exit__(None, None, None)
    with schema_editor.connection.cursor() as c:
        c.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name='aw_reporting_citystatistic' and column_name='new_id';
        """)
        column_exists = len(c.fetchall()) > 0
    if not column_exists:
        schema_editor.execute("ALTER TABLE aw_reporting_citystatistic ADD COLUMN new_id bigint;")

    with schema_editor.connection.cursor() as c:
        # Find last progress of migrating id values to new_id
        c.execute("SELECT min(id), max(id) FROM aw_reporting_citystatistic WHERE new_id IS NULL")
        min_id, max_id = c.fetchall()[0]
    # Migrate unassigned ids to new ids in batches
    if min_id is not None and max_id is not None:
        for low in range(min_id, min(max_id, 2 ** 31 - 1), 10000):
            schema_editor.execute(
                """UPDATE aw_reporting_citystatistic SET new_id = id WHERE id between %(low)s and %(high)s""",
                dict(low=low, high=low + 10000))

    schema_editor.execute("DROP INDEX IF EXISTS aw_reporting_citystatistic_pk_idx")
    schema_editor.execute(
        "CREATE UNIQUE INDEX CONCURRENTLY aw_reporting_citystatistic_pk_idx ON aw_reporting_citystatistic("
        "new_id);")

    schema_editor.execute("""
        BEGIN;
        ALTER TABLE aw_reporting_citystatistic DROP CONSTRAINT aw_reporting_citystatistic_pkey;
        CREATE SEQUENCE aw_reporting_citystatistic_new_id_seq;
        ALTER TABLE aw_reporting_citystatistic ALTER COLUMN new_id SET DEFAULT nextval( 'aw_reporting_citystatistic_new_id_seq'::regclass);
        UPDATE aw_reporting_citystatistic SET new_id = id WHERE new_id IS NULL;
        ALTER TABLE aw_reporting_citystatistic ADD CONSTRAINT aw_reporting_citystatistic_pkey PRIMARY KEY 
        USING INDEX aw_reporting_citystatistic_pk_idx;
        ALTER TABLE aw_reporting_citystatistic DROP COLUMN id;
        ALTER TABLE aw_reporting_citystatistic RENAME COLUMN new_id to id;
        ALTER SEQUENCE aw_reporting_citystatistic_new_id_seq RENAME TO aw_reporting_citystatistic_id_seq;
        SELECT setval('aw_reporting_citystatistic_id_seq', (SELECT max(id) FROM aw_reporting_citystatistic));
        COMMIT;
        """)


def reverse_code(apps, schema_editor):
    pass


class Migration(migrations.Migration):

    dependencies = [
        ('aw_reporting', '0102_auto_20201125_0144'),
    ]

    operations = [
        migrations.RunPython(change_id_to_bigint, reverse_code=reverse_code)
    ]

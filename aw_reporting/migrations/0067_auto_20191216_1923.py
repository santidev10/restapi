# Generated by Django 2.2.4 on 2019-12-16 19:23

from django.db import migrations


def change_id_to_bigint(apps, schema_editor):
    # Remove atomicity to be able to save progress of migration if timeout occurs
    schema_editor.atomic.__exit__(None, None, None)
    with schema_editor.connection.cursor() as c:
        c.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name='aw_reporting_ytchannelstatistic' and column_name='new_id';
        """)
        column_exists = len(c.fetchall()) > 0
    if not column_exists:
        schema_editor.execute("ALTER TABLE aw_reporting_ytchannelstatistic ADD COLUMN new_id bigint;")

    with schema_editor.connection.cursor() as c:
        # Find last progress of migrating id values to new_id
        c.execute("SELECT min(id), max(id) FROM aw_reporting_ytchannelstatistic WHERE new_id IS NULL")
        min_id, max_id = c.fetchall()[0]
    # Migrate unassigned ids to new ids in batches
    if min_id is not None and max_id is not None:
        for low in range(min_id, min(max_id, 2 ** 31 - 1), 10000):
            schema_editor.execute(
                """UPDATE aw_reporting_ytchannelstatistic SET new_id = id WHERE id between %(low)s and %(high)s""",
                dict(low=low, high=low + 10000))

    schema_editor.execute("DROP INDEX IF EXISTS aw_reporting_ytchannelstatistic_pk_idx")
    schema_editor.execute(
        "CREATE UNIQUE INDEX CONCURRENTLY aw_reporting_ytchannelstatistic_pk_idx ON aw_reporting_ytchannelstatistic("
        "new_id);")

    schema_editor.execute("""
        BEGIN;
        ALTER TABLE aw_reporting_ytchannelstatistic DROP CONSTRAINT aw_reporting_ytchannelstatistic_pkey;
        CREATE SEQUENCE aw_reporting_ytchannelstatistic_new_id_seq;
        ALTER TABLE aw_reporting_ytchannelstatistic ALTER COLUMN new_id SET DEFAULT nextval( 'aw_reporting_ytchannelstatistic_new_id_seq'::regclass);
        UPDATE aw_reporting_ytchannelstatistic SET new_id = id WHERE new_id IS NULL;
        ALTER TABLE aw_reporting_ytchannelstatistic ADD CONSTRAINT aw_reporting_ytchannelstatistic_pkey PRIMARY KEY 
        USING INDEX aw_reporting_ytchannelstatistic_pk_idx;
        ALTER TABLE aw_reporting_ytchannelstatistic DROP COLUMN id;
        ALTER TABLE aw_reporting_ytchannelstatistic RENAME COLUMN new_id to id;
        ALTER SEQUENCE aw_reporting_ytchannelstatistic_new_id_seq RENAME TO aw_reporting_ytchannelstatistic_id_seq;
        SELECT setval('aw_reporting_ytchannelstatistic_id_seq', (SELECT max(id) FROM aw_reporting_ytchannelstatistic));
        COMMIT;
        """)


class Migration(migrations.Migration):
    dependencies = [
        ("aw_reporting", "0066_auto_20191107_0110"),
    ]

    operations = [
        migrations.RunPython(change_id_to_bigint)
    ]

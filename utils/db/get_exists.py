from django.db import connections


def get_exists(item_ids=None, model_name=None, select_fields="id", where_id_field="id"):
    """
    Util function to check existence of ids in table
    :param item_ids: list
    :param model_name: str
    :param select_fields: sr
    :param where_id_field: str
    :return: list
    """

    if item_ids:
        parameters = ", ".join(["(%s)" for _ in item_ids])
        query = f"SELECT {select_fields} FROM {model_name} WHERE {where_id_field} = ANY (VALUES {parameters})"
        with connections['audit'].cursor() as cursor:
            cursor.execute(query, item_ids)
            rows = cursor.fetchall()
            return rows

from django.db import connections


def get_exists(item_ids=None, model_name=None, select_fields="id",
               where_id_field="id", parameters=None, conn="audit", as_dict=False):
    """
    Util function to check existence of ids in table
    :param item_ids: list
    :param model_name: str
    :param select_fields: sr
    :param where_id_field: str
    :param parameters: str
    :param conn: str
    :param as_dict: bool -> Returns list of dictionaries
    :return: list
    """
    rows = []
    if item_ids:
        parameters = parameters if parameters is not None else ", ".join(["(%s)" for _ in item_ids])
        query = f"SELECT {select_fields} FROM {model_name} WHERE {where_id_field} = ANY (VALUES {parameters})"
        with connections[conn].cursor() as cursor:
            cursor.execute(query, item_ids)
            if as_dict:
                columns = [col[0] for col in cursor.description]
                rows = [
                    dict(zip(columns, row))
                    for row in cursor.fetchall()
                ]
            else:
                rows = cursor.fetchall()
    return rows

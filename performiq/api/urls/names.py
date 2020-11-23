from utils.utils import unique_constant_tree


@unique_constant_tree
class PerformIQPathName:
    class AWAuth:
        CONNECTION_LIST = "connect_aw_account"
        CONNECTION = "aw_account_connection"

    class DV360Auth:
        CONNECTION_LIST = "connect_dv360_account"
        CONNECTION = "dv360_account_connection"

    CAMPAIGN = "campaign"
    CAMPAIGNS = "campaigns"
    MAP_CSV_FIELDS = "map_csv_fields"

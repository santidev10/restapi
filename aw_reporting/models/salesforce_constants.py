from utils.lang import ExtendedEnum as Enum


class SalesForceGoalType:
    CPM = 0
    CPV = 1
    CPM_AND_CPV = 2
    HARD_COST = 3


SalesForceGoalTypes = ("CPM", "CPV", "CPM & CPV", "Hard Cost")


def goal_type_str(goal_type_id):
    try:
        return SalesForceGoalTypes[goal_type_id]
    except (TypeError, IndexError):
        return None


class SalesForceGoalTypeStr:
    CPM = goal_type_str(SalesForceGoalType.CPM)
    CPV = goal_type_str(SalesForceGoalType.CPV)
    HARD_COST = goal_type_str(SalesForceGoalType.HARD_COST)


class DynamicPlacementType:
    BUDGET = "Budget"
    SERVICE_FEE = "Service Fee"
    RATE_AND_TECH_FEE = "Rate + Tech Fee"


ALL_DYNAMIC_PLACEMENTS = (
    DynamicPlacementType.BUDGET,
    DynamicPlacementType.SERVICE_FEE,
    DynamicPlacementType.RATE_AND_TECH_FEE
)

DYNAMIC_PLACEMENT_TYPES = (
    DynamicPlacementType.BUDGET,
    DynamicPlacementType.SERVICE_FEE,
    DynamicPlacementType.RATE_AND_TECH_FEE
)


class SalesforceFields:
    class SFAccount(Enum):
        ID = "Id"
        NAME = "Name"
        PARENT_ID = "ParentId"

    class UserRole(Enum):
        ID = "Id"
        NAME = "Name"

    class User(Enum):
        ID = "Id"
        NAME = "Name"
        PHOTO_ID = "photo_id"
        EMAIL = "Email"
        IS_ACTIVE = "IsActive"
        ROLE_ID = "UserRoleId"

    class Contact(Enum):
        ID = "Id"
        FIRST_NAME = "FirstName"
        LAST_NAME = "LastName"

    class Opportunity(Enum):
        ID = "Id"
        NAME = "Name"
        CATEGORY_ID = "Client_Vertical__c"
        TERRITORY = "Territory1__c"
        BUDGET = "Grand_Total__c"
        IO_START = "Projected_Launch_Date__c"
        START = "MIN_Placement_Start_Date__c"
        END = "MAX_Placement_End_Date__c"
        PROPOSAL_DATE = "Date_Proposal_Submitted__c"
        VIDEO_VIEWS = "CPV_Units_Purchased__c"
        IMPRESSIONS = "CPM_Impression_Units_Purchased__c"
        CPV_COST = "CPV_Total_Client_Cost__c"
        CPM_COST = "CPM_Total_Client_Cost__c"
        STAGE = "StageName"
        NUMBER = "OPP_ID_Number__c"
        AW_CID = "AdWords_CID__c"
        BRAND = "Brand_Test__c"
        AGENCY_ID = "Agency_Contact__c"
        ACCOUNT_ID = "AccountId"
        CONTRACTED_CPM = "Quoted_CPM_Price__c"
        CONTRACTED_CPV = "Avg_Cost_Per_Unit__c"
        ACCOUNT_MANAGER_ID = "Account_Manager__c"
        SALES_MANAGER_ID = "OwnerId"
        AD_OPS_MANAGER_ID = "Ad_Ops_Campaign_Manager_UPDATE__c"
        AD_OPS_QA_MANAGER_ID = "Ad_Ops_QA_Manager__c"
        CANNOT_ROLL_OVER = "DO_NOT_STRAY_FROM_DELIVERY_SCHEDULE__c"
        PROBABILITY = "Probability"
        CREATE_DATE = "CreatedDate"
        CLOSE_DATE = "CloseDate"
        RENEWAL_APPROVED = "Renewal_Approved__c"
        REASON_FOR_CLOSE = "Reason_for_Close_Lost__c"

        DEMOGRAPHIC = "Demo_TEST__c"
        GEO_TARGETING = "Geo_Targeting_Country_State_City__c"
        TARGETING_TACTICS = "Targeting_Tactics__c"
        TAGS = "Tags__c"
        TYPES_OF_TARGETING = "Types_of__c"
        APEX_DEAL = "APEX_Deal__c"
        BILLING_SERVER = "Billing_Serer__c"
        RATE_TYPE = "Rate_Type__c"
        COST_METHOD = "Cost_Method__c"

    class Placement(Enum):
        ID = "Id"
        NAME = "Name"
        OPPORTUNITY_ID = "Insertion_Order__c"
        COST_METHOD = "Cost_Method__c"
        ORDERED_UNITS = "Total_Ordered_Units__c"
        ORDERED_RATE = "Ordered_Cost_Per_Unit__c"
        TOTAL_COST = "Total_Client_Costs__c"
        START = "Placement_Start_Date__c"
        END = "Placement_End_Date__c"
        NUMBER = "PLACEMENT_ID_Number__c"
        AD_WORDS_PLACEMENT = "Adwords_Placement_IQ__c"
        INCREMENTAL = "Incremental__c"
        PLACEMENT_TYPE = "Placement_Type__c"
        DYNAMIC_PLACEMENT = "Dynamic_Placement__c"
        TECH_FEE = "Tech_Fee_if_applicable__c"
        TECH_FEE_CAP = "Tech_Fee_Cap_if_applicable__c"
        TECH_FEE_TYPE = "Tech_Fee_Type__c"

    class Flight(Enum):
        ID = "Id"
        NAME = "Name"
        PLACEMENT_ID = "Placement__c"
        START = "Flight_Start_Date__c"
        END = "Flight_End_Date__c"
        MONTH = "Flight_Month__c"

        COST = "Total_Flight_Cost__c"
        TOTAL_COST = "Flight_Value__c"
        DELIVERED = "Delivered_Ad_Ops__c"

        ORDERED_COST = "Ordered_Amount__c"
        ORDERED_UNITS = "Ordered_Units__c"
        PACING = "Pacing__c"

    class Activity(Enum):
        ID = "Id",
        NAME = "Subject"
        OWNER_ID = "OwnerId"
        TYPE = "type"
        DATE = "ActivityDate"
        OPPORTUNITY_ID = "WhatId"
        ACCOUNT_ID = "AccountId"
        ITEM_ID = "WhatId"

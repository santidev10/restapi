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
        EMAIL = "Email"
        ID = "Id"
        IS_ACTIVE = "IsActive"
        NAME = "Name"
        ROLE_ID = "UserRoleId"
        SMALL_PHOTO_URL = "SmallPhotoUrl"

    class Contact(Enum):
        FIRST_NAME = "FirstName"
        ID = "Id"
        LAST_NAME = "LastName"

    class Opportunity(Enum):
        ACCOUNT_ID = "AccountId"
        ACCOUNT_MANAGER_ID = "Account_Manager__c"
        AD_OPS_MANAGER_ID = "Ad_Ops_Campaign_Manager_UPDATE__c"
        AD_OPS_QA_MANAGER_ID = "Ad_Ops_QA_Manager__c"
        AGENCY_ID = "Agency_Contact__c"
        APEX_DEAL = "APEX_Deal__c"
        AW_CID = "AdWords_CID__c"
        BILLING_SERVER = "Billing_Serer__c"
        BRAND = "Brand_Test__c"
        BUDGET = "Grand_Total__c"
        CANNOT_ROLL_OVER = "DO_NOT_STRAY_FROM_DELIVERY_SCHEDULE__c"
        CATEGORY_ID = "Client_Vertical__c"
        CLOSE_DATE = "CloseDate"
        CONTRACTED_CPM = "Quoted_CPM_Price__c"
        CONTRACTED_CPV = "Avg_Cost_Per_Unit__c"
        COST_METHOD = "Cost_Method__c"
        CPM_COST = "CPM_Total_Client_Cost__c"
        CPV_COST = "CPV_Total_Client_Cost__c"
        CREATE_DATE = "CreatedDate"
        DEMOGRAPHIC = "Demo_TEST__c"
        END = "MAX_Placement_End_Date__c"
        GEO_TARGETING = "Geo_Targeting_Country_State_City__c"
        ID = "Id"
        IMPRESSIONS = "CPM_Impression_Units_Purchased__c"
        IO_START = "Projected_Launch_Date__c"
        NAME = "Name"
        NUMBER = "OPP_ID_Number__c"
        PROBABILITY = "Probability"
        PROPOSAL_DATE = "Date_Proposal_Submitted__c"
        RATE_TYPE = "Rate_Type__c"
        REASON_FOR_CLOSE = "Reason_for_Close_Lost__c"
        RENEWAL_APPROVED = "Renewal_Approved__c"
        SALES_MANAGER_ID = "OwnerId"
        STAGE = "StageName"
        START = "MIN_Placement_Start_Date__c"
        TAGS = "Tags__c"
        TARGETING_TACTICS = "Targeting_Tactics__c"
        TERRITORY = "Territory1__c"
        TYPES_OF_TARGETING = "Types_of__c"
        VIDEO_VIEWS = "CPV_Units_Purchased__c"

    class Placement(Enum):
        AD_WORDS_PLACEMENT = "Adwords_Placement_IQ__c"
        COST_METHOD = "Cost_Method__c"
        DYNAMIC_PLACEMENT = "Dynamic_Placement__c"
        END = "Placement_End_Date__c"
        ID = "Id"
        INCREMENTAL = "Incremental__c"
        NAME = "Name"
        NUMBER = "PLACEMENT_ID_Number__c"
        OPPORTUNITY_ID = "Insertion_Order__c"
        ORDERED_RATE = "Ordered_Cost_Per_Unit__c"
        ORDERED_UNITS = "Total_Ordered_Units__c"
        PLACEMENT_TYPE = "Placement_Type__c"
        START = "Placement_Start_Date__c"
        TECH_FEE = "Tech_Fee_if_applicable__c"
        TECH_FEE_CAP = "Tech_Fee_Cap_if_applicable__c"
        TECH_FEE_TYPE = "Tech_Fee_Type__c"
        TOTAL_COST = "Total_Client_Costs__c"

    class Flight(Enum):
        COST = "Total_Flight_Cost__c"
        DELIVERED = "Delivered_Ad_Ops__c"
        END = "Flight_End_Date__c"
        ID = "Id"
        MONTH = "Flight_Month__c"
        NAME = "Name"
        ORDERED_COST = "Ordered_Amount__c"
        ORDERED_UNITS = "Ordered_Units__c"
        PACING = "Pacing__c"
        PLACEMENT_ID = "Placement__c"
        START = "Flight_Start_Date__c"
        TOTAL_COST = "Flight_Value__c"

    class Activity(Enum):
        ACCOUNT_ID = "AccountId"
        DATE = "ActivityDate"
        ID = "Id"
        ITEM_ID = "WhatId"
        NAME = "Subject"
        OWNER_ID = "OwnerId"

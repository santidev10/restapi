import logging
import os

from django.contrib.auth import get_user_model
from django.contrib.staticfiles.templatetags.staticfiles import static
from django.db import models
from django.db.models import Count

from aw_reporting.models.base import BaseModel, BaseQueryset
from aw_reporting.models.salesforce_constants import SalesForceGoalType
from aw_reporting.models.salesforce_constants import SalesForceGoalTypes
from aw_reporting.models.salesforce_constants import SalesForceRegions
from aw_reporting.models.salesforce_constants import goal_type_str
from userprofile.managers import UserRelatedManagerMixin

logger = logging.getLogger(__name__)


class Category(BaseModel):
    id = models.CharField(max_length=200, primary_key=True)

    @property
    def name(self):
        return self.id

    def __str__(self):
        return self.id

    @classmethod
    def get_data(cls, data):
        return dict(id=data['value'])


class SFAccount(BaseModel):
    id = models.CharField(max_length=20, primary_key=True)
    name = models.CharField(max_length=200)
    parent = models.ForeignKey('self', null=True)

    @classmethod
    def get_data(cls, data):
        return dict(id=data['Id'], name=data['Name'],
                    parent_id=data['ParentId'])


class UserRole(BaseModel):
    id = models.CharField(max_length=20, primary_key=True)
    name = models.CharField(max_length=200)

    AD_OPS_NAME = "Ad Ops"
    ACCOUNT_MANAGER_NAME = "Account Manager"

    @classmethod
    def get_data(cls, data):
        return dict(id=data['Id'], name=data['Name'])


class User(BaseModel):
    id = models.CharField(max_length=20, primary_key=True)
    name = models.CharField(max_length=60, db_index=True)  # Name
    photo_id = models.CharField(max_length=255, null=True)
    email = models.EmailField(null=True)
    is_active = models.BooleanField(default=False)
    role = models.ForeignKey(UserRole, null=True, related_name="users")

    @property
    def photo_name(self):
        return "{}.jpg".format(self.photo_id)

    @property
    def photo_url(self):
        if self.photo_id:
            return static('/'.join(('img', 'sf', self.photo_name)))

    @classmethod
    def get_data(cls, data):
        return dict(id=data['Id'],
                    name=data['Name'],
                    photo_id=data.get('photo_id'),
                    email=data['Email'],
                    is_active=data['IsActive'],
                    role_id=data["UserRoleId"])

    def __str__(self):
        return self.name


class Contact(BaseModel):
    id = models.CharField(max_length=20, primary_key=True)
    first_name = models.CharField(max_length=255, null=True)
    last_name = models.CharField(max_length=255, null=True)

    @property
    def name(self):
        return " ".join(
            [str(i) for i in (self.first_name, self.last_name) if i]
        )

    def __str__(self):
        return "%s" % self.name

    @classmethod
    def get_data(cls, data):
        return dict(
            id=data['Id'],
            first_name=data['FirstName'],
            last_name=data['LastName'],
        )


class OpportunityManager(models.Manager.from_queryset(BaseQueryset), UserRelatedManagerMixin):
    _account_id_ref = "placements__adwords_campaigns__account_id"

    def have_campaigns(self, user=None):
        return self.get_queryset_for_user(user=user) \
            .annotate(campaign_count=Count("placements__adwords_campaigns")) \
            .filter(campaign_count__gt=0)


class Opportunity(models.Model):
    objects = OpportunityManager()
    id = models.CharField(max_length=20, primary_key=True)  # Id
    aw_cid = models.CharField(max_length=60, null=True)
    number = models.CharField(max_length=10, null=True)
    name = models.CharField(max_length=250, db_index=True)  # Name

    category = models.ForeignKey(Category, null=True,
                                 related_name="opportunities",
                                 on_delete=models.SET_NULL)

    region_id = models.SmallIntegerField(null=True)  # Rep_Division__c
    budget = models.FloatField(default=0)

    io_start = models.DateField(null=True)  # Projected_Launch_Date__c
    start = models.DateField(null=True)  # MIN_Placement_Start_Date__c
    end = models.DateField(null=True)  # MAX_Placement_End_Date__c
    proposal_date = models.DateField(null=True)

    # todo: remove from opportunity level
    goal_type_id = models.SmallIntegerField(default=0)
    units = models.IntegerField(default=0)

    video_views = models.IntegerField(null=True)
    impressions = models.IntegerField(null=True)
    cpv_cost = models.FloatField(null=True)
    cpm_cost = models.FloatField(null=True)

    # The data in this field tells us
    # if the result of the previous month will affect
    # the expected output of the current month.
    cannot_roll_over = models.BooleanField(default=False)

    # Total_Units__c
    stage = models.CharField(max_length=60, null=True)
    probability = models.PositiveSmallIntegerField(null=True)
    create_date = models.DateField(null=True)
    close_date = models.DateField(null=True)
    renewal_approved = models.BooleanField(default=False)
    reason_for_close = models.TextField(default="")

    # sf managers
    account_manager = models.ForeignKey(
        User, null=True, related_name="managed_opportunities",
        on_delete=models.SET_NULL
    )
    sales_manager = models.ForeignKey(
        User, null=True, related_name="sold_opportunities",
        on_delete=models.SET_NULL
    )
    ad_ops_manager = models.ForeignKey(
        User, null=True, related_name="ad_managed_opportunities",
        on_delete=models.SET_NULL
    )
    ad_ops_qa_manager = models.ForeignKey(
        User, null=True, related_name="qa_managed_opportunities",
        on_delete=models.SET_NULL
    )

    # iq fields
    ad_ops_email = models.EmailField(null=True)
    am_email = models.EmailField(null=True)
    sales_email = models.EmailField(null=True)

    notes = models.TextField(null=True, blank=True)

    brand = models.CharField(max_length=255, null=True)
    agency = models.ForeignKey(Contact, null=True)
    account = models.ForeignKey(SFAccount, null=True)

    iq_category_id = models.SmallIntegerField(null=True)
    iq_region_id = models.SmallIntegerField(null=True)

    contracted_cpm = models.FloatField(null=True)
    contracted_cpv = models.FloatField(null=True)

    demographic = models.CharField(max_length=300, default="")
    geo_targeting = models.TextField(default="")
    targeting_tactics = models.CharField(max_length=400, default="")
    tags = models.CharField(max_length=20, default="")
    types_of_targeting = models.CharField(max_length=100, default="")

    apex_deal = models.BooleanField(default=False)
    bill_of_third_party_numbers = models.BooleanField(default=False)

    default_thumbnail = None

    @property
    def thumbnail(self):
        if self.ad_ops_email:
            try:
                ad_ops = get_user_model().objects.get(
                    email=self.ad_ops_email
                )
            except get_user_model().DoesNotExist:
                pass
            else:
                if ad_ops.profile_image_url:
                    return ad_ops.profile_image_url
        return None

    def __str__(self):
        return "%s" % self.name

    @property
    def cpv(self):
        if self.video_views is not None and self.cpv_cost is not None:
            return self.cpv_cost / self.video_views

    @property
    def cpm(self):
        if self.impressions is not None and self.cpm_cost is not None:
            return self.cpm_cost / self.impressions

    @property
    def views(self):
        return self.video_views

    @property
    def goal_type(self):
        return ", ".join(self.goal_types)

    @property
    def goal_type_ids(self):
        return self.placements.order_by() \
            .values_list("goal_type_id", flat=True) \
            .distinct()

    @property
    def goal_types(self):
        goal_type_ids = self.goal_type_ids
        types_str = [
            goal_type_str(goal_type_id) for goal_type_id in goal_type_ids]
        goal_types = filter(lambda x: x is not None, types_str)
        return goal_types

    @classmethod
    def email_from_sf_id(cls, sf_id):
        try:
            user = User.objects.get(pk=sf_id)
        except User.DoesNotExist:
            pass
        else:
            return user.email

    @classmethod
    def get_data(cls, data):
        territory = data.get('Territory1__c')
        if territory:
            if territory in SalesForceRegions:
                region_id = SalesForceRegions.index(territory)
            else:
                logger.critical("{} not in SFRegions".format(territory))
                region_id = None
        else:
            region_id = None

        rate_type = data.get('Rate_Type__c') or data.get('Cost_Method__c')
        if rate_type and 'CPM' in rate_type:
            if 'CPV' in rate_type:
                goal_type_id = SalesForceGoalType.CPM_AND_CPV
            else:
                goal_type_id = SalesForceGoalType.CPM
        else:
            goal_type_id = SalesForceGoalType.CPV

        # matching sf and iq users
        sales_email = cls.email_from_sf_id(
            data.get('OwnerId')
        )
        ad_ops_email = cls.email_from_sf_id(
            data.get('Ad_Ops_Campaign_Manager_UPDATE__c')
        )
        am_email = cls.email_from_sf_id(
            data.get('Account_Manager__c')
        )

        units = 0
        if data.get('CPM_Impression_Units_Purchased__c'):
            units += data['CPM_Impression_Units_Purchased__c']
        if data.get('CPV_Units_Purchased__c'):
            units += data['CPV_Units_Purchased__c']

        res = dict(
            id=data['Id'],
            name=data.get('Name'),
            category_id=data.get('Client_Vertical__c'),
            region_id=region_id,
            budget=data.get('Grand_Total__c') or 0,
            io_start=data.get('Projected_Launch_Date__c'),
            start=data.get('MIN_Placement_Start_Date__c'),
            end=data.get('MAX_Placement_End_Date__c'),
            proposal_date=data.get('Date_Proposal_Submitted__c'),
            goal_type_id=goal_type_id,
            units=units,
            video_views=data.get('CPV_Units_Purchased__c'),
            impressions=data.get('CPM_Impression_Units_Purchased__c'),
            cpv_cost=data.get('CPV_Total_Client_Cost__c'),
            cpm_cost=data.get('CPM_Total_Client_Cost__c'),
            stage=data.get('StageName'),
            number=data.get('OPP_ID_Number__c'),
            aw_cid=data.get('AdWords_CID__c'),
            brand=data.get('Brand_Test__c'),
            agency_id=data.get('Agency_Contact__c'),
            account_id=data.get('AccountId'),
            contracted_cpm=data.get('Quoted_CPM_Price__c'),
            contracted_cpv=data.get('Avg_Cost_Per_Unit__c'),
            account_manager_id=data.get('Account_Manager__c'),
            sales_manager_id=data.get('OwnerId'),
            ad_ops_manager_id=data.get('Ad_Ops_Campaign_Manager_UPDATE__c'),
            ad_ops_qa_manager_id=data.get('Ad_Ops_QA_Manager__c'),
            cannot_roll_over=data['DO_NOT_STRAY_FROM_DELIVERY_SCHEDULE__c'],
            probability=data['Probability'],
            create_date=data['CreatedDate'].split('T')[0]
            if data['CreatedDate'] else None,
            close_date=data['CloseDate'],
            renewal_approved=data['Renewal_Approved__c'],
            reason_for_close=data['Reason_for_Close_Lost__c'] or "",

            demographic=data["Demo_TEST__c"] or "",
            geo_targeting=data["Geo_Targeting_Country_State_City__c"] or "",
            targeting_tactics=data["Targeting_Tactics__c"] or "",
            tags=data["Tags__c"] or "",
            types_of_targeting=data["Types_of__c"] or "",
            apex_deal=data.get("APEX_Deal__c"),
            bill_of_third_party_numbers=data.get("Bill_off_3p_Numbers__c")
        )
        if sales_email:
            res['sales_email'] = sales_email
        if ad_ops_email:
            res['ad_ops_email'] = ad_ops_email
        if am_email:
            res['am_email'] = am_email
        return res

    class Meta:
        ordering = ('-start',)


class OpPlacement(BaseModel):
    id = models.CharField(max_length=20, primary_key=True)
    opportunity = models.ForeignKey(Opportunity, related_name='placements')
    name = models.CharField(max_length=100)
    goal_type_id = models.SmallIntegerField(null=True)
    ordered_units = models.IntegerField(null=True)
    ordered_rate = models.FloatField(null=True)
    total_cost = models.FloatField(null=True)
    start = models.DateField(null=True)
    end = models.DateField(null=True)
    number = models.CharField(max_length=10, null=True, db_index=True)
    ad_words_placement = models.CharField(max_length=255, null=True)

    placement_type = models.CharField(max_length=25, null=True)
    dynamic_placement = models.CharField(max_length=25, null=True)

    tech_fee = models.DecimalField(max_digits=12, decimal_places=4, null=True)
    tech_fee_cap = models.DecimalField(max_digits=12, decimal_places=4,
                                       null=True)
    TECH_FEE_CPV_TYPE = "CPV"
    TECH_FEE_CPM_TYPE = "CPM"
    tech_fee_type = models.CharField(
        max_length=3, null=True,
        choices=((TECH_FEE_CPV_TYPE, TECH_FEE_CPV_TYPE),
                 (TECH_FEE_CPM_TYPE, TECH_FEE_CPM_TYPE))
    )

    max_allowed_bid = models.PositiveIntegerField(null=True)
    automation_enabled = models.BooleanField(default=False)
    incremental = models.BooleanField(default=False)

    OUTGOING_FEE_TYPE = "Outgoing Fee"

    def __str__(self):
        return "%s" % self.name

    @property
    def goal_type(self):
        if self.goal_type_id is None:
            return None
        return SalesForceGoalTypes[int(self.goal_type_id)]

    @classmethod
    def get_data(cls, data):

        goal_type_id = None
        cost_method = data['Cost_Method__c']
        if cost_method:
            if cost_method in SalesForceGoalTypes:
                goal_type_id = SalesForceGoalTypes.index(cost_method)
            elif 'CPV' in cost_method:
                goal_type_id = SalesForceGoalType.CPV
            elif 'CPM' in cost_method:
                goal_type_id = SalesForceGoalType.CPM

        res = dict(
            id=data['Id'],
            name=data['Name'],
            opportunity_id=data['Insertion_Order__c'],
            goal_type_id=goal_type_id,
            ordered_units=data['Total_Ordered_Units__c'],
            ordered_rate=data['Ordered_Cost_Per_Unit__c'],
            total_cost=data['Total_Client_Costs__c'],
            start=data['Placement_Start_Date__c'],
            end=data['Placement_End_Date__c'],
            number=data['PLACEMENT_ID_Number__c'],
            ad_words_placement=data['Adwords_Placement_IQ__c'],
            incremental=data['Incremental__c'],
            placement_type=data['Placement_Type__c'],
            dynamic_placement=data['Dynamic_Placement__c'],
            tech_fee=data["Tech_Fee_if_applicable__c"],
            tech_fee_cap=data["Tech_Fee_Cap_if_applicable__c"],
            tech_fee_type=data["Tech_Fee_Type__c"],
        )
        return res

    class Meta:
        ordering = ('name',)

    @property
    def video_views(self):
        if self.goal_type_id == SalesForceGoalType.CPV:
            return self.ordered_units

    @property
    def impressions(self):
        if self.goal_type_id == SalesForceGoalType.CPM:
            return self.ordered_units


class Flight(BaseModel):
    id = models.CharField(max_length=20, primary_key=True)
    placement = models.ForeignKey(OpPlacement, related_name='flights')
    name = models.CharField(max_length=100)

    start = models.DateField(null=True)
    end = models.DateField(null=True)
    month = models.SmallIntegerField(null=True)

    cost = models.FloatField(null=True)
    delivered = models.IntegerField(null=True)

    ordered_cost = models.FloatField(null=True)
    total_cost = models.FloatField(null=True)
    ordered_units = models.IntegerField(null=True)

    class Meta:
        ordering = ('start',)

    def __str__(self):
        return "%s" % self.name

    def __init__(self, *args, **kwargs):
        super(Flight, self).__init__(*args, **kwargs)
        self._stats = None

    @property
    def stats(self):
        if self._stats is None:
            filters = {}
            if self.start:
                filters['statistics__date__gte'] = self.start
            if self.end:
                filters['statistics__date__lte'] = self.end

            stat_query_set = self.placement.adwords_campaigns.all()

            if filters:
                stat_query_set = stat_query_set.filter(**filters)

            campaign_stats = stat_query_set.aggregate(
                video_views=models.Sum('statistics__video_views'),
                impressions=models.Sum('statistics__impressions'),
                cost=models.Sum('statistics__cost'),
            )
            self._stats = campaign_stats
        return self._stats

    @property
    def delivered_cost(self):
        return self.stats['cost']

    @property
    def delivered_units(self):
        goal_type_id = self.placement.goal_type_id

        if goal_type_id == SalesForceGoalType.CPM:
            return self.stats['impressions']
        elif goal_type_id == SalesForceGoalType.CPV:
            return self.stats['video_views']

    @classmethod
    def get_data(cls, data):

        res = dict(
            id=data['Id'],
            name=data['Name'],
            placement_id=data['Placement__c'],
            start=data['Flight_Start_Date__c'],
            end=data['Flight_End_Date__c'],
            month=data['Flight_Month__c'],

            cost=data['Total_Flight_Cost__c'],
            total_cost=data['Flight_Value__c'],
            delivered=data['Delivered_Ad_Ops__c'],

            ordered_cost=data['Ordered_Amount__c'],
            ordered_units=data['Ordered_Units__c'],
        )
        return res


class Activity(BaseModel):
    id = models.CharField(max_length=20, primary_key=True)
    owner = models.ForeignKey(User, related_name='activities')
    name = models.CharField(max_length=250)
    type = models.CharField(max_length=10, db_index=True)
    date = models.DateField()

    opportunity = models.ForeignKey(
        Opportunity, related_name='activities', null=True)
    account = models.ForeignKey(
        SFAccount, related_name='activities', null=True)

    EMAIL_TYPE = "email"
    MEETING_TYPE = "meeting"

    @classmethod
    def get_data(cls, data):
        item_id = data['WhatId']
        res = dict(
            id=data['Id'],
            name=data['Subject'] or "",
            owner_id=data['OwnerId'],
            type=data['type'],
            date=data['ActivityDate'],
            opportunity_id=item_id
            if item_id and item_id.startswith('006') else None,
            account_id=data['AccountId'],
        )
        return res

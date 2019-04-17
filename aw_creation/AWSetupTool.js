var IQ_API_HOST = "https://rc-viewiq.channelfactory.com/api/v1/";
var GET_ACCOUNT_IDS_URL = "webhook_aw/get_accounts_list/";
var SAVE_SETTINGS_URL = "webhook_aw/save_settings/";

function main() {
    var mccAccount = AdWordsApp.currentAccount();

    while(1){
        var ids = get_account_ids(get_account_id());
        if(ids.length < 1){
            break;
        }
        var accountSelector = MccApp.accounts().withIds(ids).get();
        while(accountSelector.hasNext()){
            var account = accountSelector.next();
            MccApp.select(account);
            save_campaign_settings();
        }
        MccApp.select(mccAccount);
    }
}
function get_account_ids(manager_id){
    var options = {
        muteHttpExceptions : true,
        method: "GET",
    };
    var resp = UrlFetchApp.fetch(IQ_API_HOST + GET_ACCOUNT_IDS_URL + manager_id + '/', options);
    if(resp.getResponseCode() == 200) {
        return JSON.parse(resp.getContentText());
    }else{
        Logger.log(resp.getResponseCode());
        Logger.log(resp.getContentText());
        return '';
    }
}
function get_account_id(){
    return AdWordsApp.currentAccount().getCustomerId().split('-').join('');
}
function save_campaign_settings(){
    var account_id = get_account_id();
    Logger.log("account id: ", account_id);
    var campaign_settings = get_campaign_settings(account_id);

    var options = {
        muteHttpExceptions : true,
        method: "PUT",
        payload: JSON.stringify({campaigns: campaign_settings}),
        contentType: 'application/json',
    };
    var resp = UrlFetchApp.fetch(IQ_API_HOST + SAVE_SETTINGS_URL + account_id + '/', options);
    if(resp.getResponseCode() == 200) {
        return resp.getContentText();
    }else{
        Logger.log(resp.getResponseCode());
        Logger.log(resp.getContentText());
        return '';
    }
}

function get_campaign_settings(account_id){
    var settings = [];

    var campaignIterator = AdWordsApp.videoCampaigns().get();
    while(campaignIterator.hasNext()) {
        var campaign = campaignIterator.next();

        var tt_statuses = [];
      	var videoAdIterator = campaign.videoAds().withCondition("Status != DISABLED").get();
      	while (videoAdIterator.hasNext()) {
        	var videoAd = videoAdIterator.next();
            var tracking_template = videoAd.urls().getTrackingTemplate();
            tt_statuses.push(!!tracking_template);
        }

        var campaign_settings = {
          id: campaign.getId(),
          name: campaign.getName(),
          start: campaign.getStartDate(),
          end: campaign.getStartDate(),
          is_removed: campaign.isRemoved(),
          is_paused: campaign.isPaused(),
          is_enabled: campaign.isEnabled(),
          tracking_template_is_set: tt_statuses.length && tt_statuses.reduce(add, 0) == tt_statuses.length,
          age_ranges: [],
          genders: [],
          locations: [],
        };
        settings.push(campaign_settings);

        var target_locations = campaign.targeting().targetedLocations().get();
        while (target_locations.hasNext()) {
            var item = target_locations.next();
            campaign_settings.locations.push(item.getId());
        }

        var video_targeting = campaign.videoTargeting();

      	var agesIterator = video_targeting.ages().get();
        while (agesIterator.hasNext()) {
           var age = agesIterator.next().getAgeRange();
           if(campaign_settings.age_ranges.indexOf(age) == -1){
           	  campaign_settings.age_ranges.push(age);
           }
        }

        var gendersIterator = video_targeting.genders().get();
        while (gendersIterator.hasNext()) {
           var gender = gendersIterator.next().getGenderType();
           if(campaign_settings.genders.indexOf(gender) == -1){
           	  campaign_settings.genders.push(gender);
           }
        }

        var has_interest_targeting = false;
        var has_remarketing_targeting = false;
        var has_custom_affinity_targeting = false;

        var ad_groups = campaign.videoAdGroups().get();
        while(ad_groups.hasNext()){
            var ad_group_targeting = ad_groups.next().videoTargeting();
            var selector = ad_group_targeting.audiences().get();
            while(selector.hasNext()){
                var next = selector.next();
                var audience_type = next.getAudienceType();
                if (audience_type == "USER_LIST"){
                    has_remarketing_targeting = true;
                }else if (audience_type == "CUSTOM_AFFINITY"){
                    has_custom_affinity_targeting = true;
                }else{
                    has_interest_targeting = true;
                }
            }
        }

        // targeting
        campaign_settings["targeting_interests"] = has_interest_targeting;
        campaign_settings["targeting_custom_affinity"] = has_custom_affinity_targeting;
        campaign_settings["targeting_remarketings"] = has_remarketing_targeting;
        campaign_settings["targeting_topics"] = video_targeting.topics().get().hasNext();
        campaign_settings["targeting_keywords"] = video_targeting.keywords().get().hasNext();
        campaign_settings["targeting_channels"] = video_targeting.youTubeChannels().get().hasNext();
        campaign_settings["targeting_videos"] = video_targeting.youTubeVideos().get().hasNext();

        // Excluded targeting
        var excluded_channels = video_targeting.excludedYouTubeChannels().get();
        campaign_settings["targeting_excluded_channels"] = excluded_channels.hasNext();

        var excluded_topics = video_targeting.excludedTopics().get();
        campaign_settings["targeting_excluded_topics"] = excluded_topics.hasNext();

        var excluded_keywords = video_targeting.excludedKeywords().get();
        campaign_settings["targeting_excluded_keywords"] = excluded_keywords.hasNext();
    }

    return settings;
}

function add(a, b) {
    return a + b;
}
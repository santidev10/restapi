function getOrCreateCampaign(params, created) {
    var campaignIterator = AdsApp.campaigns().withCondition('Name CONTAINS "#' + params.id + '"').get();
  	var videoCampaignIterator = AdsApp.videoCampaigns().withCondition('Name CONTAINS "#' + params.id + '"').get();

    if (campaignIterator.hasNext()) {
        var campaign = campaignIterator.next();
    } else if (videoCampaignIterator.hasNext()) {
        var campaign = videoCampaignIterator.next();
    } else if (params.is_deleted) {
        return null;
    } else {
        if (created) {
            throw Error("Can't create campaign " + JSON.stringify(params, null, 2));
        }
        createCampaign(params.name, params.budget, params.start_for_creation, params.bid_strategy_type, params.campaign_type, params.target_cpa);
        Utilities.sleep(5000);
        campaign = getOrCreateCampaign(params, true);
    }
    return campaign;
}

// Google scripts implicitly changes:
// 'cpm' to Maximum CPM
// 'target cpa' to Target CPA
// 'cpv' to Maximum CPV
function mapBidType(bid_type) {
    if (bid_type === "target_cpa"){
        return "target cpa";
    } else if (bid_type === "target_cpm") {
     	return "target cpm";
    } else if (bid_type === "max_cpm") {
    	return "cpm";
    } else if (bid_type === "max_cpv") {
     	return 'cpv';
    }
    return bid_type;
}

function createCampaign(name, budget, start, bid_strategy_type, campaign_type, target_cpa) {
    var columns = ['Campaign', 'Budget', 'Start Date', 'Bid Strategy type',
        'Campaign type', 'Campaign state'];

    var options = {
        'Campaign': name,
        'Budget': budget,
        'Start Date': start,
        'Bid Strategy type': mapBidType(bid_strategy_type),
        'Campaign type': campaign_type,
        'Campaign state': 'paused',
    };

    if (bid_strategy_type === 'target_cpa') {
        columns.push('Target CPA');
        options['Target CPA'] = target_cpa;
    }

    var upload = AdsApp.bulkUploads().newCsvUpload(columns);
    Logger.log("creation\n" + JSON.stringify(options, null, 2));
    upload.append(options);
    upload.apply();
}

var DEVICE_NAMES = {HighEndMobile: 'MOBILE_DEVICE', Desktop: 'DESKTOP_DEVICE', Tablet: 'TABLET_DEVICE'};

function createOrUpdateCampaign(params){

    var campaign = getOrCreateCampaign(params);

    if ( ! campaign){
        return null;
    }

    campaign.setName(params.name);

    if(params.is_paused || params.is_deleted){
        campaign.pause();
    }else{
        campaign.enable();
    }

    var start_date = date_to_string(campaign.getStartDate());
    if(params.start && start_date != params.start){
        campaign.setStartDate(params.start);
    }
    var end_date = date_to_string(campaign.getEndDate());
    if(params.end && end_date != params.end){
        campaign.setEndDate(params.end);
    }

    var budget_obj = campaign.getBudget();
    if (params.budget_type === "total") {
        budget_obj.setTotalAmount(params.budget);
    } else {
        budget_obj.setAmount(params.budget);
    }

    var targeting = campaign.targeting();
    var languages = targeting.languages().get();
    while(languages.hasNext()) {
        var lang = languages.next();
        var index = params.lang_ids.indexOf(lang.getId());
        if(index == -1){
            lang.remove();
        }else{
            params.lang_ids.splice(index, 1);
        }
    }
    for(var i=0;i<params.lang_ids.length;i++){
        campaign.addLanguage(params.lang_ids[i]);
    }

    var platforms = targeting.platforms().get();
    while(platforms.hasNext()) {
        var platform = platforms.next();
        var name = DEVICE_NAMES[platform.getName()];
        if(params.devices.indexOf(name) == -1){
            platform.setBidModifier(0);
        }else{
            platform.setBidModifier(1);
        }
    }

    var ad_schedules = targeting.adSchedules().get();
    while (ad_schedules.hasNext()) {
        var s = ad_schedules.next();
        var repr = [s.getDayOfWeek(), s.getStartHour(), s.getStartMinute(), s.getEndHour(), s.getEndMinute()].join(" ");
        var index = params.schedules.indexOf(repr);
        if(index == -1){
            s.remove();
        }else{
            params.schedules.splice(index, 1);
        }
    }
    for(var i=0;i<params.schedules.length;i++){
        var s = params.schedules[i].split(" ");;
        campaign.addAdSchedule({
            dayOfWeek: s[0],
            startHour: parseInt(s[1]),
            startMinute: parseInt(s[2]),
            endHour: parseInt(s[3]),
            endMinute: parseInt(s[4]),
            bidModifier: 1,
        });
    }

    var target_locations = targeting.targetedLocations().get();
    while (target_locations.hasNext()) {
        var item = target_locations.next();
        var index = params.locations.indexOf(item.getId());
        if(index == -1){  // item isn't in the list
            item.remove();
        }else{
            params.locations.splice(index, 1);
        }
    }

    for(var i=0;i<params.locations.length;i++){
        campaign.addLocation(params.locations[i]);
    }

    var target_proximities = targeting.targetedProximities().get();
    while (target_proximities.hasNext()) {
        var item = target_proximities.next();
        var repr = [item.getLatitude(), item.getLongitude(), item.getRadius(), item.getRadiusUnits()].join(" ");
        var index = params.proximities.indexOf(repr);
        if(index == -1){
            item.remove();
        }else{
            params.proximities.splice(index, 1);
        }
    }

    for(var i=0;i<params.proximities.length;i++){
        var args = params.proximities[i].split(" ");
        campaign.addProximity(parseFloat(args[0]), parseFloat(args[1]), parseInt(args[2]), args[3]);
    }

    var exclusions_iterator = targeting.excludedContentLabels().get();
    while(exclusions_iterator.hasNext()) {
        var exclusion = exclusions_iterator.next();
        var index = params.content_exclusions.indexOf(exclusion.getId());
        if(index == -1){
            exclusion.remove();
        }else{
            params.content_exclusions.splice(index, 1);
        }
    }

    for(var i=0;i<params.content_exclusions.length;i++){
        campaign.excludeContentLabel(params.content_exclusions[i]);
    }

    if (params.bid_strategy_type !== 'target_cpa') {
        campaign.setNetworks(params.video_networks);

        var saved_caps = campaign.getFrequencyCaps();
        var cap_types = ["IMPRESSION", "VIDEO_VIEW"];
        for(var i=0;i<cap_types.length;i++){
            var type = cap_types[i];
            var isc = saved_caps.getFrequencyCapFor(type);
            var im_cap = params.freq_caps[type];
            if(!isc || !im_cap || isc.getLevel() != im_cap['level'] || isc.getTimeUnit() != im_cap['time_unit'] || isc.getLimit() != im_cap['limit']){
                if(isc){
                    Logger.log("Drop freq cap " + type);
                    saved_caps.removeFrequencyCapFor(type);
                }
                if(im_cap){
                    Logger.log("Create freq cap " + type);
                    var builder = saved_caps.newFrequencyCapBuilder();
                    builder.withEventType(type).withLevel(im_cap['level']).withTimeUnit(im_cap['time_unit']).withLimit(im_cap['limit']).build();
                }
            }
        }
    }
    return campaign;
}

function getOrCreateAdGroup(campaign, params){
  	if (params.bid_strategy_type === 'target_cpa') {
      var ad_groups = campaign.adGroups().withCondition('Name CONTAINS "#' + params.id + '"').get();
    } else {
      var ad_groups = campaign.videoAdGroups().withCondition('Name CONTAINS "#' + params.id + '"').get();
    }

    if(ad_groups.hasNext()) {
        var ad_group = ad_groups.next();
    } else if ( params.is_deleted ){
        return null;
    }else{
        // CPA campaigns do not use videoAdGroups
      	if (params.bid_strategy_type === 'target_cpa') {
          var builder = campaign.newAdGroupBuilder().withName(params.name);
        } else {
		      var builder = campaign.newVideoAdGroupBuilder().withName(params.name).withAdGroupType(params.ad_format);
        }

      	if (params.bid_strategy_type === 'target_cpa') {
          	builder = builder.withCpa(params.target_cpa);
        } else if (params.ad_format == "VIDEO_BUMPER") {
            builder = builder.withCpm(params.max_rate);
        } else {
            builder = builder.withCpv(params.max_rate);
        }

        builder.build();

        Utilities.sleep(30000);

        ad_group = getOrCreateAdGroup(campaign, params);

        if (params.bid_strategy_type !== 'target_cpa') {
            // targeting initialization
            var targeting = ad_group.videoTargeting();
            GENDERS = ['GENDER_FEMALE', 'GENDER_MALE', 'GENDER_UNDETERMINED'];
            var builder = targeting.newGenderBuilder();
            for (var i = 0; i < GENDERS.length; i++) {
                builder.withGenderType(GENDERS[i]).build();
            }
            PARENTS = ['PARENT_PARENT', 'PARENT_NOT_A_PARENT', 'PARENT_UNDETERMINED'];
            var builder = targeting.newParentalStatusBuilder();
            for (var i = 0; i < PARENTS.length; i++) {
                builder.withParentType(PARENTS[i]).build();
            }
            AGE_RANGES = ['AGE_RANGE_18_24', 'AGE_RANGE_25_34', 'AGE_RANGE_35_44', 'AGE_RANGE_45_54', 'AGE_RANGE_55_64', 'AGE_RANGE_65_UP', 'AGE_RANGE_UNDETERMINED'];
            var builder = targeting.newAgeBuilder();
            for (var i = 0; i < AGE_RANGES.length; i++) {
                builder.withAgeRange(AGE_RANGES[i]).build();
            }
        }
    }
    return ad_group
}

function createOrUpdateAdGroup(campaign, params){
    if ( ! campaign){
        return null;
    }
    var ad_group = getOrCreateAdGroup(campaign, params);
    if ( ! ad_group){
        return null;
    }

    if(params.is_deleted){
        ad_group.pause();
    }

    ad_group.setName(params.name);

    var bidding = ad_group.bidding();


    if (params.bid_strategy_type === 'target_cpa'){
       bidding.setCpa(params.target_cpa);
    } else if (params.ad_format == "VIDEO_BUMPER") {
       bidding.setCpm(params.max_rate);
    } else {
        bidding.setCpv(params.max_rate);
    }

    if (params.bid_strategy_type === 'target_cpa') {
      // Add positive keywords
      params.keywords.forEach(function(keyword, index) {
        ad_group.newKeywordBuilder()
        	.withText(keyword)
        	.build();
      });
      // Add negative keywords
      params.keywords_negative.forEach(function(negativeKeyword) {
        ad_group.createNegativeKeyword(negativeKeyword);
      });
      // Add topics by Id
      params.topics.forEach(function(topicId) {
        ad_group.display().newTopicBuilder()
        	.withTopicId(topicId)
        	.build();
      });
      // Exclude topics by Id
      params.topics_negative.forEach(function(topicId) {
        ad_group.display().newTopicBuilder()
        	.withTopicId(topicId)
        	.exclude();
      });
      // Add Audience Interests by Id
      params.interests.forEach(function(interestId) {
        ad_group.display().newAudienceBuilder()
        	.withAudienceType('USER_INTEREST')
        	.withAudienceId(interestId)
        	.build();
      });
      // Exclude Audience Interests by Id
      params.interests_negative.forEach(function(interestId) {
        ad_group.display().newAudienceBuilder()
        	.withAudienceType('USER_INTEREST')
        	.withAudienceId(interestId)
        	.exclude();
      });

      // Google Ads Scripts Demographic targeting currently only supports USER_INTEREST_AND_LIST
      // https://developers.google.com/google-ads/scripts/docs/reference/adsapp/adsapp_adgrouptargeting#setTargetingSetting_2
      ad_group.targeting().setTargetingSetting('USER_INTEREST_AND_LIST', 'TARGET_ALL_FALSE');

    } else {
        // Configure CPV / CPM Video Adgroups
        var targeting = ad_group.videoTargeting();
        //genders
        var iterator = targeting.excludedGenders().withCondition("GenderType IN [" + params.genders.join() + "]").get();
        while (iterator.hasNext()) {
            var item = iterator.next();
            item.include();
        }
        var iterator = targeting.genders().withCondition("GenderType NOT_IN [" + params.genders.join() + "]").get();
        while (iterator.hasNext()) {
            var item = iterator.next();
            item.exclude();
        }
        // parental
        var iterator = targeting.excludedParentalStatuses().withCondition("ParentType IN [" + params.parents.join() + "]").get();
        while (iterator.hasNext()) {
            var item = iterator.next();
            item.include();
        }
        var iterator = targeting.parentalStatuses().withCondition("ParentType NOT_IN [" + params.parents.join() + "]").get();
        while (iterator.hasNext()) {
            var item = iterator.next();
            item.exclude();
        }
        // age ranges
        var iterator = targeting.excludedAges().withCondition("AgeRangeType IN [" + params.ages.join() + "]").get();
        while (iterator.hasNext()) {
            var item = iterator.next();
            item.include();
        }
        var iterator = targeting.ages().withCondition("AgeRangeType NOT_IN [" + params.ages.join() + "]").get();
        while (iterator.hasNext()) {
            var item = iterator.next();
            item.exclude();
        }

        // channels and channels_negative
        var selector = targeting.youTubeChannels().get();
        while (selector.hasNext()) {
            var item = selector.next();
            var index = params.channels.indexOf(item.getChannelId());
            if (index == -1) {
                item.remove();
            } else {
                params.channels.splice(index, 1);
            }
        }
        var selector = targeting.excludedYouTubeChannels().get();
        while (selector.hasNext()) {
            var item = selector.next();
            var index = params.channels_negative.indexOf(item.getChannelId());
            if (index == -1) {
                item.remove();
            } else {
                params.channels_negative.splice(index, 1);
            }
        }

        var builder = targeting.newYouTubeChannelBuilder();
        for (var i = 0; i < params.channels.length; i++) {
            builder.withChannelId(params.channels[i]).build();
        }
        for (var i = 0; i < params.channels_negative.length; i++) {
            builder.withChannelId(params.channels_negative[i]).exclude();
        }

        // videos, videos_negative
        var selector = targeting.youTubeVideos().get();
        while (selector.hasNext()) {
            var item = selector.next();
            var index = params.videos.indexOf(item.getVideoId());
            if (index == -1) {
                item.remove();
            } else {
                params.videos.splice(index, 1);
            }
        }
        var selector = targeting.excludedYouTubeVideos().get();
        while (selector.hasNext()) {
            var item = selector.next();
            var index = params.videos_negative.indexOf(item.getVideoId());
            if (index == -1) {
                item.remove();
            } else {
                params.videos_negative.splice(index, 1);
            }
        }

        var builder = targeting.newYouTubeVideoBuilder();
        for (var i = 0; i < params.videos.length; i++) {
            builder.withVideoId(params.videos[i]).build();
        }
        for (var i = 0; i < params.videos_negative.length; i++) {
            builder.withVideoId(params.videos_negative[i]).exclude();
        }

        //topics, topics_negative
        var selector = targeting.topics().get();
        while (selector.hasNext()) {
            var item = selector.next();
            var index = params.topics.indexOf(item.getTopicId());
            if (index == -1) {
                item.remove();
            } else {
                params.topics.splice(index, 1);
            }
        }
        var selector = targeting.excludedTopics().get();
        while (selector.hasNext()) {
            var item = selector.next();
            var index = params.topics_negative.indexOf(item.getTopicId());
            if (index == -1) {
                item.remove();
            } else {
                params.topics_negative.splice(index, 1);
            }
        }

        var builder = targeting.newTopicBuilder();
        for (var i = 0; i < params.topics.length; i++) {
            builder.withTopicId(params.topics[i]).build();
        }
        for (var i = 0; i < params.topics_negative.length; i++) {
            builder.withTopicId(params.topics_negative[i]).exclude();
        }

        // interests, interests_negative
        var selector = targeting.audiences().get();
        while (selector.hasNext()) {
            var item = selector.next();
            var index = params.interests.indexOf(parseInt(item.getAudienceId()));
            if (index == -1) {
                item.remove();
            } else {
                params.interests.splice(index, 1);
            }
        }
        var selector = targeting.excludedAudiences().get();
        while (selector.hasNext()) {
            var item = selector.next();
            var index = params.interests_negative.indexOf(parseInt(item.getAudienceId()));
            if (index == -1) {
                item.remove();
            } else {
                params.interests_negative.splice(index, 1);
            }
        }

        var builder = targeting.newAudienceBuilder().withAudienceType('USER_INTEREST');
        for (var i = 0; i < params.interests.length; i++) {
            builder.withAudienceId(params.interests[i]).build();
        }
        for (var i = 0; i < params.interests_negative.length; i++) {
            builder.withAudienceId(params.interests_negative[i]).exclude();
        }

        //keywords, keywords_negative
        var selector = targeting.keywords().get();
        while (selector.hasNext()) {
            var item = selector.next();
            var index = params.keywords.indexOf(item.getText());
            if (index == -1) {
                item.remove();
            } else {
                params.keywords.splice(index, 1);
            }
        }
        var selector = targeting.excludedKeywords().get();
        while (selector.hasNext()) {
            var item = selector.next();
            var index = params.keywords_negative.indexOf(item.getText());
            if (index == -1) {
                item.remove();
            } else {
                params.keywords_negative.splice(index, 1);
            }
        }

        var builder = targeting.newKeywordBuilder();
        for (var i = 0; i < params.keywords.length; i++) {
            builder.withText(params.keywords[i]).build();
        }
        for (var i = 0; i < params.keywords_negative.length; i++) {
            builder.withText(params.keywords_negative[i]).exclude();
        }
    }

    return ad_group;
}

function getYTId(url){
    var matches = url.match(/(?:https?:\/{2})?(?:w{3}\.)?youtu(?:be)?\.(?:com|be)(?:\/watch\?v=|\/video\/|\/)([^\s&\?\/]+)/);
    if(matches != null) {
        return matches[1];
    } else {
        Logger.log("The youtube url is not valid:" + url);
        return null;
    }
}

function getOrCreateVideo(video_id){
    var videos = AdsApp.adMedia().media().withCondition("Type = VIDEO AND YouTubeVideoId = '" + video_id + "'").get();
    if(videos.hasNext()){
        var video = videos.next();
    }else{
        var operation = AdsApp.adMedia().newVideoBuilder().withYouTubeVideoId(video_id).build();
        video = operation.getResult();
    }
    return video;
}

function getOrCreateImage(image_url){
    var images = AdsApp.adMedia().media().withCondition("Type = IMAGE AND Name = '" + image_url + "'").get();
    if(images.hasNext()){
         var image = images.next();
    }else{
      		Logger.log('creating image')
         var imageBlob = UrlFetchApp.fetch(image_url).getBlob();
         var mediaOperation = AdsApp.adMedia().newImageBuilder()
            .withName(image_url).withData(imageBlob).build();
         var image = mediaOperation.getResult();
    }
    return image;
}

var AD_TYPE_IN_STREAM = "VIDEO_TRUE_VIEW_IN_STREAM";
var AD_TYPE_DISCOVERY = "VIDEO_TRUE_VIEW_IN_DISPLAY";
var AD_TYPE_BUMPER = "VIDEO_BUMPER";
var AD_TYPE_DISPLAY = "VIDEO_DISPLAY";

function getYTVideo(url) {
    var videoId = getYTId(url);
    return getOrCreateVideo(videoId);
}

function createDisplayResponsive(adGroup, params) {
  var video = getYTVideo(params.video_url);
  var builder = adGroup.newAd()
    .responsiveDisplayAdBuilder()
  	.withBusinessName(params.business_name)
    .withShortHeadline(params.short_headline)
    .withLongHeadline(params.long_headline)
    .withDescription(params.description_1)
    .withTrackingTemplate(params.tracking_template)
    .withFinalUrl(params.final_url)

    try {
      builder = builder.withCustomParameters(params.custom_params)
    } catch (err) {
      Logger.log("Error->" + err);
    }

    var marketingImageUrl = 'https://drive.google.com/uc?export=view&id=1eeMgUUp6dg9GyezWTSDOTMevgPmUlvvE';
    var marketingImage = getOrCreateImage(marketingImageUrl);

    builder = builder
      .withMarketingImage(marketingImage);

  	return builder.build();
}

function createInStreamVideoAd(adGroup, params) {
  var video = getYTVideo(params.video_url);

  var builder = adGroup.newVideoAd()
    .inStreamAdBuilder()
    .withAdName(params.name)
    .withDisplayUrl(params.display_url)
    .withTrackingTemplate(params.tracking_template)
    .withFinalUrl(params.final_url)
    .withVideo(video);

  try {
    builder = builder.withCustomParameters(params.custom_params)
  } catch (err) {
    Logger.log("Error->" + err);
  }

  if (params['video_thumbnail']) {
    var imageMedia = getOrCreateImage(params['video_thumbnail']);
    builder = builder.withCompanionBanner(imageMedia);
  }

  return builder.build()
}

function createBumperVideoAd(adGroup, params) {
    var video = getYTVideo(params.video_url);
    var builder = adGroup.newVideoAd()
        .bumperAdBuilder()
        .withAdName(params.name)
        .withDisplayUrl(params.display_url)
        .withTrackingTemplate(params.tracking_template)
        .withFinalUrl(params.final_url)
        .withVideo(video);
    try {
        builder = builder.withCustomParameters(params.custom_params)
    } catch (err) {
        Logger.log("Error->" + err);
    }
    if (params['video_thumbnail']) {
        var imageMedia = getOrCreateImage(params['video_thumbnail']);
        builder = builder.withCompanionBanner(imageMedia);
    }
    return builder.build()
}

function createDiscoveryVideoAd(adGroup, params) {
    var video = getYTVideo(params.video_url);
    var builder = adGroup.newVideoAd()
        .videoDiscoveryAdBuilder()
        .withAdName(params.name)
        .withHeadline(params.headline)
        .withDescription1(params.description_1)
        .withDescription2(params.description_2)
        .withThumbnail("DEFAULT")
        .withVideo(video);
    return builder.build()
}

function createVideoAd(adGroup, params) {
    var adFormat = params.ad_format;
    switch (adFormat) {
      case AD_TYPE_IN_STREAM:
          return createInStreamVideoAd(adGroup, params);
      case AD_TYPE_DISCOVERY:
          return createDiscoveryVideoAd(adGroup, params);
      case AD_TYPE_BUMPER:
          return createBumperVideoAd(adGroup, params);
      case AD_TYPE_DISPLAY:
          return createDisplayResponsive(adGroup, params);
      default:
          Logger.log("Unknown ad format: " + adFormat);
    }
}

function removeVideoAds(adGroup, params) {
  	if (params.ad_type === 'display') {
      var iterator = adGroup.ads().get();
      // drop if exists
      while (iterator.hasNext()) {
        var ad = iterator.next();
        var labels = ad.labels().get();

        while (labels.hasNext()) {
          var label = label.next();
          if (label.getName().indexOf("#" + params.id) !== -1) {
            ad.remove();
          }
        }
      }
    } else {
      var iterator = adGroup.videoAds().get();

      // drop if exists
      while (iterator.hasNext()) {
          var videoAd = iterator.next();
          if (videoAd.getName().indexOf("#" + params.id) !== -1) {
              videoAd.remove();
          }
      }
    }
}

function createOrUpdateVideoAd(adGroup, params) {
    if (!adGroup) {  // if there is no ad_group we don't need to manage ads within it
        return null;
    }
    removeVideoAds(adGroup, params);
    if (params.is_deleted) {
        return;
    }
    return createVideoAd(adGroup, params);
}

function getBaseCampaignsInfo() {
    var campaigns = [];
    var campaignIterator = AdsApp.campaigns().get();
    while (campaignIterator.hasNext()) {
        var campaign = campaignIterator.next();
        var ad_groups = [];
        var agIterator = campaign.adGroups().get();
        while (agIterator.hasNext()) {
            var ad_group = agIterator.next();
            ad_groups.push({id: ad_group.getId(), name: ad_group.getName()});
        }
        campaigns.push({id: campaign.getId(), name: campaign.getName(), ad_groups: ad_groups});
    }
    return campaigns;
}

function sendChangesStatus(account_id, updated_at){
    if (!AdsApp.getExecutionInfo().isPreview()) {
        var options = {
            muteHttpExceptions : true,
            method: "PATCH",
            payload: JSON.stringify({updated_at: updated_at, campaigns: getBaseCampaignsInfo()})
        };
        var resp = UrlFetchApp.fetch(IQ_API_HOST + CHANGES_STATUS_PATH + account_id + '/', options);
        if(resp.getResponseCode() == 200) {
            return resp.getContentText();
        }else{
            Logger.log(resp.getResponseCode());
            Logger.log(resp.getContentText());
            return '';
        }
    }
}

function date_to_string(date_obj){
    if(date_obj == null){
        return "";
    }
    var string = String(date_obj.year)
    if(date_obj.month < 10){
        string += "0"
    }
    string += date_obj.month
    if(date_obj.day < 10){
        string += "0"
    }
    string += date_obj.day
    return string
}
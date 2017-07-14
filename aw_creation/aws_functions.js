function getOrCreateCampaign(uid, name, budget, start, type){
    var videoCampaignIterator = AdWordsApp.videoCampaigns().withCondition('Name CONTAINS "#' + uid + '"').get();
    if(videoCampaignIterator.hasNext()) {
        Logger.log('Campaign exists:' + name);
        var campaign = videoCampaignIterator.next();
    }else{
        Logger.log('Campaign creation:' + name);
        createCampaign(name, budget, start, type);
        Utilities.sleep(5000);
        campaign = getOrCreateCampaign(uid, name, budget, start, type);
    }
    return campaign;
}

function createCampaign(name, budget, start, type) {
    var columns = ['Campaign', 'Budget', 'Start Date', 'Bid Strategy type',
        'Campaign type', 'Campaign state'];
    var upload = AdWordsApp.bulkUploads().newCsvUpload(columns);
    upload.append({
        'Campaign': name,
        'Budget': budget,
        'Start Date': start,
        'Bid Strategy type': type,
        'Campaign type': 'Video',
        'Campaign state': 'paused',
    });
    upload.apply();
}

var DEVICE_NAMES = {HighEndMobile: 'MOBILE_DEVICE', Desktop: 'DESKTOP_DEVICE', Tablet: 'TABLET_DEVICE'};

function createOrUpdateCampaign(uid, name, budget, start_for_creation, budget_type, is_paused, start, end, video_networks, lang_ids, devices, schedules, freq_caps, locations, proximities){

    var campaign = getOrCreateCampaign(uid, name, budget, start_for_creation, budget_type);

    campaign.setName(name);

    if(is_paused){
        campaign.pause();
    }else{
        campaign.enable();
    }

    var start_date = date_to_string(campaign.getStartDate());
    if(start_date != start){
        campaign.setStartDate(start);
    }
    var end_date = date_to_string(campaign.getEndDate());
    if(end_date != end){
        campaign.setEndDate(end);
    }

    var budget_obj = campaign.getBudget();
    budget_obj.setAmount(budget);

    campaign.setNetworks(video_networks);


    var targeting = campaign.targeting();
    var languages = targeting.languages().get();
    while(languages.hasNext()) {
        var lang = languages.next();
        var index = lang_ids.indexOf(lang.getId());
        if(index == -1){
            lang.remove();
        }else{
            lang_ids.splice(index, 1);
        }
    }
    for(var i=0;i<lang_ids.length;i++){
        campaign.addLanguage(lang_ids[i]);
    }

    var platforms = targeting.platforms().get();
    while(platforms.hasNext()) {
        var platform = platforms.next();
        var name = DEVICE_NAMES[platform.getName()];
        if(devices.indexOf(name) == -1){
            platform.setBidModifier(0);
        }else{
            platform.setBidModifier(1);
        }
    }

    var ad_schedules = targeting.adSchedules().get();
    while (ad_schedules.hasNext()) {
        var s = ad_schedules.next();
        var repr = [s.getDayOfWeek(), s.getStartHour(), s.getStartMinute(), s.getEndHour(), s.getEndMinute()].join(" ");
        var index = schedules.indexOf(repr);
        if(index == -1){
            s.remove();
        }else{
            schedules.splice(index, 1);
        }
    }
    for(var i=0;i<schedules.length;i++){
        var s = schedules[i].split(" ");;
        campaign.addAdSchedule({
            dayOfWeek: s[0],
            startHour: parseInt(s[1]),
            startMinute: parseInt(s[2]),
            endHour: parseInt(s[3]),
            endMinute: parseInt(s[4]),
            bidModifier: 1,
        });
    }

    var saved_caps = campaign.getFrequencyCaps();
    var cap_types = ["IMPRESSION", "VIDEO_VIEW"];
    for(var i=0;i<cap_types.length;i++){
        var type = cap_types[i];
        var isc = saved_caps.getFrequencyCapFor(type);
        var im_cap = freq_caps[type];
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

    var target_locations = targeting.targetedLocations().get();
    while (target_locations.hasNext()) {
        var item = target_locations.next();
        var index = locations.indexOf(item.getId());
        if(index == -1){  // item isn't in the list
            item.remove();
        }else{
            locations.splice(index, 1);
        }
    }
    for(var i=0;i<locations.length;i++){
        campaign.addLocation(locations[i]);
    }


    var target_proximities = targeting.targetedProximities().get();
    while (target_proximities.hasNext()) {
        var item = target_proximities.next();
        var repr = [item.getLatitude(), item.getLongitude(), item.getRadius(), item.getRadiusUnits()].join(" ");
        var index = proximities.indexOf(repr);
        if(index == -1){
            item.remove();
        }else{
            proximities.splice(index, 1);
        }
    }
    for(var i=0;i<proximities.length;i++){
        Logger.log(proximities[i]);
        var args = proximities[i].split(" ");
        campaign.addProximity(parseFloat(args[0]), parseFloat(args[1]), parseInt(args[2]), args[3]);
    }

    return campaign;
}

function getOrCreateAdGroup(campaign, uid, name, ad_format, cpv){
    var ad_groups = campaign.videoAdGroups().withCondition('Name CONTAINS "#' + uid + '"').get();
    if(ad_groups.hasNext()) {
        var ad_group = ad_groups.next();
    }else{
        campaign.newVideoAdGroupBuilder().withName(name).withAdGroupType(ad_format).withCpv(cpv).build();
        Utilities.sleep(5000);
        ad_group = getOrCreateAdGroup(campaign, uid, name, ad_format, cpv);

        // targeting initialization
        var targeting = ad_group.videoTargeting();
        GENDERS = ['GENDER_FEMALE', 'GENDER_MALE', 'GENDER_UNDETERMINED'];
        var builder = targeting.newGenderBuilder();
        for(var i=0;i<GENDERS.length;i++){
            builder.withGenderType(GENDERS[i]).build();
        }
        PARENTS = ['PARENT_PARENT', 'PARENT_NOT_A_PARENT', 'PARENT_UNDETERMINED'];
        var builder = targeting.newParentalStatusBuilder();
        for(var i=0;i<PARENTS.length;i++){
            builder.withParentType(PARENTS[i]).build();
        }
        AGE_RANGES = ['AGE_RANGE_18_24', 'AGE_RANGE_25_34', 'AGE_RANGE_35_44', 'AGE_RANGE_45_54', 'AGE_RANGE_55_64', 'AGE_RANGE_65_UP', 'AGE_RANGE_UNDETERMINED'];
        var builder = targeting.newAgeBuilder();
        for(var i=0;i<AGE_RANGES.length;i++){
            builder.withAgeRange(AGE_RANGES[i]).build();
        }
    }
    return ad_group
}

function createOrUpdateAdGroup(campaign, uid, name, ad_format, cpv, genders, parents, ages, channels, channels_negative, videos, videos_negative, topics, topics_negative, interests, interests_negative, keywords, keywords_negative){
    var ad_group = getOrCreateAdGroup(campaign, uid, name, ad_format, cpv);

    ad_group.setName(name);
    var bidding = ad_group.bidding();
    bidding.setCpv(cpv);

    var targeting = ad_group.videoTargeting();
    //genders
    var iterator = targeting.excludedGenders().withCondition("GenderType IN [" + genders.join() + "]").get();
    while (iterator.hasNext()) {
        var item = iterator.next();
        item.include();
    }
    var iterator = targeting.genders().withCondition("GenderType NOT_IN [" + genders.join() + "]").get();
    while (iterator.hasNext()) {
        var item = iterator.next();
        item.exclude();
    }
    // parental
    var iterator = targeting.excludedParentalStatuses().withCondition("ParentType IN [" + parents.join() + "]").get();
    while (iterator.hasNext()) {
        var item = iterator.next();
        item.include();
    }
    var iterator = targeting.parentalStatuses().withCondition("ParentType NOT_IN [" + parents.join() + "]").get();
    while (iterator.hasNext()) {
        var item = iterator.next();
        item.exclude();
    }
    // age ranges
    var iterator = targeting.excludedAges().withCondition("AgeRangeType IN [" + ages.join() + "]").get();
    while (iterator.hasNext()) {
        var item = iterator.next();
        item.include();
    }
    var iterator = targeting.ages().withCondition("AgeRangeType NOT_IN [" + ages.join() + "]").get();
    while (iterator.hasNext()) {
        var item = iterator.next();
        item.exclude();
    }

    // channels and channels_negative
    var selector = targeting.youTubeChannels().get();
    while (selector.hasNext()) {
        var item = selector.next();
        var index = channels.indexOf(item.getChannelId());
        if(index == -1){
            item.remove();
        }else{
            channels.splice(index, 1);
        }
    }
    var selector = targeting.excludedYouTubeChannels().get();
    while (selector.hasNext()) {
        var item = selector.next();
        var index = channels_negative.indexOf(item.getChannelId());
        if(index == -1){
            item.remove();
        }else{
            channels_negative.splice(index, 1);
        }
    }

    var builder = targeting.newYouTubeChannelBuilder();
    for(var i=0;i<channels.length;i++){
        builder.withChannelId(channels[i]).build();
    }
    for(var i=0;i<channels_negative.length;i++){
        builder.withChannelId(channels_negative[i]).exclude();
    }

    // videos, videos_negative
    var selector = targeting.youTubeVideos().get();
    while (selector.hasNext()) {
        var item = selector.next();
        var index = videos.indexOf(item.getVideoId());
        if(index == -1){
            item.remove();
        }else{
            videos.splice(index, 1);
        }
    }
    var selector = targeting.excludedYouTubeVideos().get();
    while (selector.hasNext()) {
        var item = selector.next();
        var index = videos_negative.indexOf(item.getVideoId());
        if(index == -1){
            item.remove();
        }else{
            videos_negative.splice(index, 1);
        }
    }

    var builder = targeting.newYouTubeVideoBuilder();
    for(var i=0;i<videos.length;i++){
        builder.withVideoId(videos[i]).build();
    }
    for(var i=0;i<videos_negative.length;i++){
        builder.withVideoId(videos_negative[i]).exclude();
    }

    //topics, topics_negative
    var selector = targeting.topics().get();
    while (selector.hasNext()) {
        var item = selector.next();
        var index = topics.indexOf(item.getTopicId());
        if(index == -1){
            item.remove();
        }else{
            topics.splice(index, 1);
        }
    }
    var selector = targeting.excludedTopics().get();
    while (selector.hasNext()) {
        var item = selector.next();
        var index = topics_negative.indexOf(item.getTopicId());
        if(index == -1){
            item.remove();
        }else{
            topics_negative.splice(index, 1);
        }
    }

    var builder = targeting.newTopicBuilder();
    for(var i=0;i<topics.length;i++){
        builder.withTopicId(topics[i]).build();
    }
    for(var i=0;i<topics_negative.length;i++){
        builder.withTopicId(topics_negative[i]).exclude();
    }

    // interests, interests_negative
    var selector = targeting.audiences().get();
    while (selector.hasNext()) {
        var item = selector.next();
        var index = interests.indexOf(parseInt(item.getAudienceId()));
        if(index == -1){
            item.remove();
        }else{
            interests.splice(index, 1);
        }
    }
    var selector = targeting.excludedAudiences().get();
    while (selector.hasNext()) {
        var item = selector.next();
        var index = interests_negative.indexOf(parseInt(item.getAudienceId()));
        if(index == -1){
            item.remove();
        }else{
            interests_negative.splice(index, 1);
        }
    }

    var builder = targeting.newAudienceBuilder().withAudienceType('USER_INTEREST');
    for(var i=0;i<interests.length;i++){
        builder.withAudienceId(interests[i]).build();
    }
    for(var i=0;i<interests_negative.length;i++){
        builder.withAudienceId(interests_negative[i]).exclude();
    }

    //keywords, keywords_negative
    var selector = targeting.keywords().get();
    while (selector.hasNext()) {
        var item = selector.next();
        var index = keywords.indexOf(item.getText());
        if(index == -1){
            item.remove();
        }else{
            keywords.splice(index, 1);
        }
    }
    var selector = targeting.excludedKeywords().get();
    while (selector.hasNext()) {
        var item = selector.next();
        var index = keywords_negative.indexOf(item.getText());
        if(index == -1){
            item.remove();
        }else{
            keywords_negative.splice(index, 1);
        }
    }

    var builder = targeting.newKeywordBuilder();
    for(var i=0;i<keywords.length;i++){
        builder.withText(keywords[i]).build();
    }
    for(var i=0;i<keywords_negative.length;i++){
        builder.withText(keywords_negative[i]).exclude();
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
    var videos = AdWordsApp.adMedia().media().withCondition("Type = VIDEO AND YouTubeVideoId = '" + video_id + "'").get();
    if(videos.hasNext()){
        var video = videos.next();
    }else{
        var operation = AdWordsApp.adMedia().newVideoBuilder().withYouTubeVideoId(video_id).build();
        video = operation.getResult();
    }
    return video;
}

function createOrUpdateVideoAd(ad_group, video_url, display_url, final_url){
    display_url = display_url || video_url
    final_url = final_url || video_url
    var video_id = getYTId(video_url);
    var video = getOrCreateVideo(video_id);

    var not_exists = true;
    var iterator = ad_group.videoAds().get();
    while (iterator.hasNext()) {
        var video_ad = iterator.next();
        var current_final_url = video_ad.urls().getFinalUrl();
        if(video_ad.getVideoId() == video_id && video_ad.getDisplayUrl() == display_url && current_final_url == final_url){
            not_exists = false;
        }else{
            video_ad.remove();
        }
    }
    if(not_exists){
        var ad_builder = ad_group.newVideoAd().inStreamAdBuilder().withAdName("InStreamAd")
        .withDisplayUrl(display_url)
        .withFinalUrl(final_url).withVideo(video).build();
    }
}

function sendChangesStatus(account_id, code){
    if (!AdWordsApp.getExecutionInfo().isPreview()) {
        var options = {
            muteHttpExceptions : true,
            method: "PATCH",
            payload:{is_changed:false},
        };
        var resp = UrlFetchApp.fetch(IQ_API_HOST + CHANGES_STATUS + account_id + '/' + code + '/', options);
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
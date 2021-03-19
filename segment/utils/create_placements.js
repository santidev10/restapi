// request code and ctl urls from viewiq, or csv from s3


function getadGroups(campaignName) {
    var condition = 'CampaignName CONTAINS ' + "'" + campaignName + "'"
    return AdsApp.videoadGroups().withCondition(condition).get()
}

function createPlacements(adGroupIterator, urls) {
    while (adGroupIterator.hasNext()) {
        var adGroup = adGroupIterator.next()
        createPlacement(adGroup, urls)
    }
}

function createPlacement(adGroup, urls) {
    var builder = adGroup.videoTargeting().newPlacementBuilder();
    urls.forEach(function(url) {
        var operation = builder.withUrl(url).build();
    })
}
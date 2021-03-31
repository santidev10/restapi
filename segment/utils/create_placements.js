function run() {
  var adGroupIds = {adGroupIds}
  var placementIds = {placementIds}

  var adGroupIterator = getAdGroups(adGroupIds);
  while (adGroupIterator.hasNext()) {
    var adGroup = adGroupIterator.next();
    removeExistingPlacements(adGroup);
    createPlacements(adGroup, placementIds);
  }
}


function getAdGroups(adGroupIds) {
    return AdsApp.videoAdGroups().withIds(adGroupIds).get();
}

function createPlacements(adGroup, placementIds) {
  var placementBuilder = adGroup.videoTargeting()['newYouTubeChannelBuilder']();

  placementIds.forEach(function(id) {
    placementBuilder['withChannelId'](id).build();
  });
}

function removeExistingPlacements(adGroup) {
  // Remove all existing placements before adding new ones
  var placementsIterator = adGroup.videoTargeting()['youTubeChannels']().get();
  while (placementsIterator.hasNext()) {
    placementsIterator.next().remove();
  }
}

function updateSyncStatus(ctl_id) {
  var options = {
    'muteHttpExceptions' : true,
    'method': 'PATCH',
    'payload': JSON.stringify({ ctl_id: ctl_id }),
   	'contentType': 'application/json'
  };
  var url = getSyncUrl()
  var resp = UrlFetchApp.fetch(url, options);
  var message;

  if (resp.getResponseCode() == 200) {
    message = JSON.parse(resp.getContentText());
  } else {
    message = {
  	  'errorCode': resp.getResponseCode(),
      'message': resp.getContentText()
    };
  }
  return message;
}


function getSyncUrl() {
  var SYNC_ENDPOINT = '{DOMAIN}/api/v2/segments/sync/'
  var cid = AdsApp.currentAccount().getCustomerId().split('-').join('');
  var url = SYNC_ENDPOINT + cid + '/';
  return url
}
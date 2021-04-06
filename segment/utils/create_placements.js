function run() {
  var ctlData = {CTL_DATA}
  ctlData.Object.keys(function(ctlId) {
    var adgroupIds = ctlData[ctlId].adGroupIds;
    var placementIds = ctlData[ctlId].placementIds;
    var placementType = ctlData[ctlId].placementType;

    var adGroupIterator = getAdGroups(adgroupIds);
    while (adGroupIterator.hasNext()) {
      var adGroup = adGroupIterator.next();
      removeExistingPlacements(adGroup, placementType);
      createPlacements(adGroup, placementIds, placementType);
    }
  })
}


function getAdGroups(adGroupIds) {
    return AdsApp.videoAdGroups().withIds(adGroupIds).get();
}

function createPlacements(adGroup, placementIds, placementType) {
  var builderName = 'newYouTube' + placementType + 'Builder';
  var builderIdName = 'with' + placementType + 'Id';
  var placementBuilder = adGroup.videoTargeting()[builderName]();

  placementIds.forEach(function(id) {
    placementBuilder[builderIdName](id).build();
  });
}

function removeExistingPlacements(adGroup, placementType) {
  // Remove all existing placements before adding new ones
  var targetingName = 'youTube' + placementType + 's';
  var placementsIterator = adGroup.videoTargeting()[targetingName]().get();
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
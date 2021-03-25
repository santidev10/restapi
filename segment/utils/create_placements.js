var SYNC_ENDPOINT = '{DOMAIN}' + '/api/v2/segments/sync/'

function main() {
  // get code from viewiq to eval here
  // var code = UrlFetchApp.fetch();
  // var data = getData();
  // var adGroupIds = {adGroupIds}
  // var placementIds = {placementIds}
  var adGroupIds = [97595304713];
  var placementIds = ["UCJ5v_MCY6GNUBTO8-D3XoAg"]

  var adGroupIterator = getAdGroups(adGroupIds);
  //var ag = adGroupIterator.next();
  //removeExistingPlacements(ag, false);
  createPlacements(adGroupIterator, placementIds, config);
}

function getData() {
  var cid = AdsApp.currentAccount().getCustomerId().split("-").join("");
  var url = SYNC_ENDPOINT + "?cid=" + cid;
  var response = UrlFetchApp.fetch(url);
  var data = JSON.parse(response.getContentText());
  return data
}


function getAdGroups(adGroupIds) {
    return AdsApp.videoAdGroups().withIds(adGroupIds).get();
}

function createPlacements(adGroupIterator, placementIds) {
  while (adGroupIterator.hasNext()) {
    var adGroup = adGroupIterator.next();
    var placementBuilder = adGroup.videoTargeting()['{placementBuilderType}']();

    placementIds.forEach(function(id) {
      placementBuilder['{placementIdType}'](id).build();
    });
  }
}

function removeExistingPlacements(adGroup) {
  // Remove all existing placements before adding new ones
  var placementsIterator = adGroup.videoTargeting()['{placementRemovalType}']().get();
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

  var resp = UrlFetchApp.fetch(SYNC_ENDPOINT, options);
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

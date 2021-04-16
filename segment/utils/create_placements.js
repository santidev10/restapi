function run() {
  var ctlData = {CTL_DATA}
  // Max allowed sleep per pollPlacementDeletion call. Two calls for a max of 20 min
  var maxSleep = 1000 * 60 * 10;
  Object.keys(ctlData).forEach(function(ctlId) {
    var adgroupIds = ctlData[ctlId].adgroupIds;
    var placementIds = ctlData[ctlId].placementIds;
    var placementType = ctlData[ctlId].placementType;

    var adGroupIterator = getAdGroups(adgroupIds);
    while (adGroupIterator.hasNext()) {
      var adGroup = adGroupIterator.next();
      removeExistingPlacements(adGroup);
      pollPlacementDeletion(adGroup, 'Channel', maxSleep);
      pollPlacementDeletion(adGroup, 'Video', maxSleep);
      createPlacements(adGroup, placementIds, placementType);
    }
    if (!AdsApp.getExecutionInfo().isPreview()) {
      updateSyncStatus(adgroupIds);
    }
  });
}

function getAdGroups(adGroupIds) {
    return AdsApp.videoAdGroups().withIds(adGroupIds).get();
}

function pollPlacementDeletion(adGroup, placementType, maxSleep) {
  var targetingName = 'youTube' + placementType + 's';
  var slept = 0;
  var step = 0;
  while (slept <= maxSleep) {
    var placementsIterator = adGroup.videoTargeting()[targetingName]().get();
    if (!placementsIterator.hasNext()) {
      break;
    }
    var sleeping = (Math.pow(2, step) + Math.random() * 1000) / 1000;
    // 300000 is max Utilities sleep limit
    sleeping = Math.min(sleeping, 300000);
    Utilities.sleep(sleeping);
    step += 1
    slept += sleeping
  }
  return true;
}

function createPlacements(adGroup, placementIds, placementType) {
  var builderName = 'newYouTube' + placementType + 'Builder';
  var builderIdName = 'with' + placementType + 'Id';
  var placementBuilder = adGroup.videoTargeting()[builderName]();

  placementIds.forEach(function(id) {
    placementBuilder[builderIdName](id).build();
  });
}

function removeExistingPlacements(adGroup) {
  // Remove all existing placements before adding new ones
  var channelPlacementsIterator = adGroup.videoTargeting().youTubeChannels().forDateRange('ALL_TIME').get()
  while (channelPlacementsIterator.hasNext()) {
    channelPlacementsIterator.next().remove();
  }

  var videoPlacementsIterator = adGroup.videoTargeting().youTubeVideos().forDateRange('ALL_TIME').get();
  while (videoPlacementsIterator.hasNext()) {
    videoPlacementsIterator.next().remove();
  }
}

function updateSyncStatus(adgroupIds) {
  var options = {
    'muteHttpExceptions' : true,
    'method': 'PATCH',
    'payload': JSON.stringify({ adgroup_ids: adgroupIds }),
   	'contentType': 'application/json'
  };
  var url = getSyncUrl()
  var resp = UrlFetchApp.fetch(url, options);
  var message;

  if (resp.getResponseCode() !== 200) {
    message = {
  	  'errorCode': resp.getResponseCode(),
      'message': resp.getContentText()
    };
    Logger.log(message);
  }
}

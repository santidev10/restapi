const baseAPIUrl = 'https://www.viewiq.com/api/v1';
const syncResource = '/google_ads/';

function main() {
  var ids = [1821053756];
  if(ids.length > 0) {
    var accountSelector = MccApp.accounts().withCondition("CustomerId IN [" + ids.join(",") + "]");
    accountSelector.executeInParallel('initChanges');
  }
}

function getChangedAccounts() {
  const data = [{
    "name": "Campaign ANOTHER TEST #1",
    "budget": "123.00",
    "start_date": "2020-04-20",
    "bid_strategy_type": "cpv",
    "type": "video",
    "status": "paused",
  }];
}

function initChanges() {


  Object.keys(resourceConfigs).forEach(function(configKey) {
    const config = resourceConfigs[configKey];
    createOrUpdate(data, config.columnMapping);
  });
}

function createOrUpdate(items, columnMapping) {
  const columns = Object.keys(columnMapping).map(function(key) {
    return columnMapping[key];
  });
  const data = extractData(items, columnMapping);
  applyBulkUpload(data, columns);
}

function extractData(data, columnMapping) {
  return data.map(function(item) {
      var params = {};
      Object.keys(item).map(function(key) {
        const column = columnMapping[key]
        params[column] = item[key]
      });
      return params;
  });
}

function applyBulkUpload(data, columns) {
    var bulkUploadManager = AdsApp.bulkUploads().newCsvUpload(columns);
    data.forEach(function(item) {
       bulkUploadManager.append(item);
    });
    bulkUploadManager.apply();
}


function updateSyncTimes(campaignIds) {
  var options = {
    'muteHttpExceptions' : true,
    'method': 'PATCH',
    'payload': JSON.stringify({ campaignIds: campaignIds }),
   	'contentType': 'application/json'
  };

  var resp = UrlFetchApp.fetch(IQ_API_HOST + CAMPAIGNS_SYNCED, options);
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

function request(url, options) {
  var resp = UrlFetchApp.fetch(IQ_API_HOST + CAMPAIGNS_SYNCED, options);
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
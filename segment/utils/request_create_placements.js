/*
This snippet of code should be pasted in client Google Ads account scripts to support updating
placements for Google Ads resources
 */
VIQ_KEY = ""

function main() {
  try {
    AdsManagerApp.accounts();
    mccRun();
  } catch {
    run();
  }
}

function mccRun() {
  // Requests cid ids that contain adgroups to be synced and executes run function for each individual cid account
  var url = getSyncUrl() + '&as_mcc';
  var cids = JSON.parse(UrlFetchApp.fetch(url).getContentText());
  // 50 is max number of accounts to process in parallel
  var accountSelector = AdsManagerApp.accounts().withLimit(50).withIds(cids);
  accountSelector.executeInParallel("run")
}

function run() {
  // Retrieve execution code to create placements for adgroups under current cid account
  var url = getSyncUrl();
  var response = JSON.parse(UrlFetchApp.fetch(url).getContentText());
  eval(response.code)
}

function getSyncUrl() {
  var SYNC_ENDPOINT = 'https://www.viewiq.com/api/v2/segments/sync/gads/'
  var cid = AdsApp.currentAccount().getCustomerId().split('-').join('');
  var url = SYNC_ENDPOINT + cid + '/' + '?viq_key=' + VIQ_KEY
  return url
}

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
  var url = getSyncUrl() + '&as_mcc';
  var cids = JSON.parse(UrlFetchApp.fetch(url).getContentText());
  var accountSelector = AdsManagerApp.accounts().withLimit(50).withIds(cids);
  accountSelector.executeInParallel("run")
}

function run() {
  var url = getSyncUrl();
  var code = JSON.parse(UrlFetchApp.fetch(url).getContentText());
  eval(code)
}

function getSyncUrl() {
  var SYNC_ENDPOINT = 'https://www.viewiq.com/api/v2/segments/sync/'
  var cid = AdsApp.currentAccount().getCustomerId().split('-').join('');
  var url = SYNC_ENDPOINT + cid + '/' + '?viq_key=' + VIQ_KEY
  return url
}

VIQ_KEY = '{VIQ_KEY}'

function main() {
  try {
    AdsManagerApp.accounts();
    mccRun();
  } catch (err) {
    run();
  }
}

function mccRun() {
  var url = getSyncUrl() + '&as_mcc=true';
  var cids = JSON.parse(UrlFetchApp.fetch(url).getContentText());
  var accountSelector = AdsManagerApp.accounts().withLimit(50).withIds(cids);
  accountSelector.executeInParallel('run')
}

function run() {
  var url = getSyncUrl();
  var response = JSON.parse(UrlFetchApp.fetch(url).getContentText());
  eval(response.code + 'run()');
}

function getSyncUrl() {
  var SYNC_ENDPOINT = 'https://viewiq.com/api/v2/segments/sync/gads/'
  var cid = AdsApp.currentAccount().getCustomerId().split('-').join('');
  var url = SYNC_ENDPOINT + cid + '/' + '?viq_key=' + VIQ_KEY
  return url
}


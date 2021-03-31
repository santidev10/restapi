/*
This snippet of code should be pasted in client Google Ads account scripts to support updating
placements for Google Ads resources
 */

function main() {
  var url = 'https://www.viewiq.com/api/v2/segments/sync/' + AdsApp.currentAccount().getCustomerId().split("-").join("");
  var code = JSON.parse(UrlFetchApp.fetch(url).getContentText());
  eval(code)
}

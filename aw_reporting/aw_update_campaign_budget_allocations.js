// Need to get endpoint from backend, get flight campaigns with new budget allocations
/*
1. get camaign ids to edit with their new budgets from backend
2. Retrieve campaigns
3. Iterate through campaigns and set new budget amounts
*/

var IQ_API_HOST = "https://rc.view-iq.channelfactory.com/api/v1/";
var CHANGED_ACCOUNTS = "aw_creation_changed_accounts_list/";
var CODE_ENDPOINT = "aw_creation_code/";
var CHANGES_STATUS_PATH = "aw_creation_changes_status/";

function main() {
  Logger.log('Updating budget allocations...');
  // Object with keys -> campaign id, value -> newBudget
  var mcc_account_id = get_mcc_account_id();
  //var accountsToUpdate = getUpdatedCampaignBudgets(mcc_account_id);

  var accountsToUpdate = {
    "accountIds": [
      "1191514178",
      "7155851537",
      "9102949537"
    ],
    "campaignBudgets": {
      "2902": 2,
      "1687597595": 6
    }
  }

  //Logger.log(campaign_ids);
  var accountIterator = AdsManagerApp.accounts()
  .withIds(accountsToUpdate.accountIds)
  .get()

  processAccounts(accountIterator, accountsToUpdate);

  //updateCampaignBudgets(campaignIterator);
  Logger.log('Budget allocations update complete');
}

function get_mcc_account_id(){
  return AdWordsApp.currentAccount().getCustomerId().split('-').join('');
}

function getUpdatedCampaignBudgets() {
  // Retrieve accounts that have been edited
  var options = {
    muteHttpExceptions : true,
    method: "GET",
  };
  var resp = UrlFetchApp.fetch(IQ_API_HOST + CHANGED_ACCOUNTS + '/', options);
  if (resp.getResponseCode() == 200) {
    return JSON.parse(resp.getContentText());
  } else {
    Logger.log(resp.getResponseCode());
    Logger.log(resp.getContentText());
    return '';
  }
}

function processAccounts(iterator, accountsToUpdate) {
  var accountIterator = iterator;

  while (accountIterator.hasNext()) {
    var account = accountIterator.next();

    AdsManagerApp.select(account);

    var campaignIterator = AdsApp.videoCampaigns()
    .withIds(Object.keys(accountsToUpdate.campaignBudgets))
    .get()

    processCampaigns(campaignIterator, accountsToUpdate.campaignBudgets)
  }
}

function processCampaigns(iterator, campaignBudgets) {
  var campaignIterator = iterator;

  while (campaignIterator.hasNext()) {
    var campaign = campaignIterator.next();
    var campaignBudget = campaign.getBudget();

    campaignBudget.setAmount(campaignBudgets[campaign.getId()]);
    Logger.log('Done: ' + campaign.getName());
  }
}

function getManagedAccounts() {
  return AdsManagerApp.accounts.get();
}





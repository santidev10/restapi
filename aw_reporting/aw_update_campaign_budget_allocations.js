var IQ_API_HOST = 'https://rc.view-iq.channelfactory.com/api/v1/';
var CHANGED_ACCOUNTS = 'pacing_report/flights/campaigns/budgets/updated/';
var SET_CAMPAIGN_ACCOUNT_UPDATE_TIMES = 'pacing_report/status/'

function main() {
  Logger.log('Updating budget allocations...');
  // Object with keys -> campaign id, value -> newBudget
  var mcc_account_id = get_mcc_account_id();
  var accountsToUpdate = getUpdatedAccounts(mcc_account_id);

  Logger.log(accountsToUpdate)

  var accountIterator = AdsManagerApp
      .accounts()
      .withIds(accountsToUpdate.accountIds)
      .get()

  processAccounts(accountIterator, accountsToUpdate);

  var accountIds = accountsToUpdate.accountIds;
  var hourlyUpdatedAt = accountsToUpdate.hourlyUpdatedAt;

  setAccountCampaignUpdateTimes(accountIds, campaignIds, hourlyUpdatedAt)

  Logger.log('Budget allocations update complete');
}

function get_mcc_account_id(){
  return AdWordsApp.currentAccount().getCustomerId().split('-').join('');
}

function getUpdatedAccounts(mcc_account_id) {
  // Retrieve aw_accounts that have been edited / marked for syncing in view-iq
  var options = {
    muteHttpExceptions : true,
    method: 'GET',
  };
  var resp = UrlFetchApp.fetch(IQ_API_HOST + CHANGED_ACCOUNTS + mcc_account_id + '/', options);

  if (resp.getResponseCode() == 200) {

    return JSON.parse(resp.getContentText());

  } else {

    Logger.log(resp.getResponseCode());
    Logger.log(resp.getContentText());

    return '';
  }
}

function processAccounts(iterator, accountsToUpdate) {
  // Iterate over received accounts and process their campaigns
  var accountIterator = iterator;

  while (accountIterator.hasNext()) {
    var account = accountIterator.next();

    AdsManagerApp.select(account);

    var campaignIterator = AdsApp.videoCampaigns()
      .withIds(Object.keys(accountsToUpdate.campaignBudgets))
      .get()

    // For each account, pass its campaigns as an iterator to process
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

function setAccountCampaignUpdateTimes(accountIds, updatedAt) {
  // Update Account and Campaign object hourly_updated_at fields to mark sync with Adwords
  var options = {
    muteHttpExceptions : true,
    method: 'PATCH',
    data: {
      account_ids: accountIds,
      hourly_updated_at: updatedAt
    }
  };

  var resp = UrlFetchApp.fetch(IQ_API_HOST + SET_CAMPAIGN_ACCOUNT_UPDATE_TIMES + '/', options);

  Logger.log(resp.data)

  if (resp.getResponseCode() == 200) {

    return JSON.parse(resp.getContentText());

  } else {

    Logger.log(resp.getResponseCode());
    Logger.log(resp.getContentText());

    return '';
  }
}





var IQ_API_HOST = 'https://rc.view-iq.channelfactory.com/api/v1/';
var CHANGED_ACCOUNTS = 'pacing_report/flights/campaigns/budgets/updated/';
var SET_CAMPAIGN_ACCOUNT_UPDATE_TIMES = 'pacing_report/status/'

function main() {
  Logger.log('Updating budget allocations...');

  var mcc_account_id = getAccountId();
  var accountsToUpdate = getUpdatedAccounts(mcc_account_id);
  Logger.log(JSON.stringify(accountsToUpdate))
  var accountSelector = AdsManagerApp
      .accounts()
      .withIds(accountsToUpdate.accountIds);

  accountSelector.executeInParallel('processAccount', 'displayResults', JSON.stringify(accountsToUpdate));

  Logger.log('Budget allocations update complete');
}

function getAccountId() {
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

function processAccount(budgetConfig) {
  var cidAccount = getAccountId();
  budgetConfig = JSON.parse(budgetConfig);

  var campaignIterator = AdsApp.videoCampaigns()
      .withIds(Object.keys(budgetConfig.campaignBudgets))
      .get();

  processCampaigns(campaignIterator, budgetConfig.campaignBudgets);

  var hourlyUpdatedAt = budgetConfig.hourlyUpdatedAt;
  var response = setAccountCampaignUpdateTimes(cidAccount, hourlyUpdatedAt);

  return response;
}

function processCampaigns(iterator, campaignBudgets) {
  var campaignIterator = iterator;

  while (campaignIterator.hasNext()) {
    var campaign = campaignIterator.next();
    var campaignBudget = campaign.getBudget();

    var updatedCampaignBudget = campaignBudgets[campaign.getId()];

    if (updatedCampaignBudget) {
      campaignBudget.setAmount(updatedCampaignBudget);
    }
  }
}

function getManagedAccounts() {
  return AdsManagerApp.accounts.get();
}

function setAccountCampaignUpdateTimes(accountId, updatedAt) {
  // Update Accounts hourly_updated_at fields to mark sync with Adwords
  var options = {
    muteHttpExceptions : true,
    method: 'PATCH',
    data: {
      account_id: accountId,
      updated_at: updatedAt
    }
  };

  var resp = UrlFetchApp.fetch(IQ_API_HOST + SET_CAMPAIGN_ACCOUNT_UPDATE_TIMES + '/', options);

  if (resp.getResponseCode() == 200) {
    return JSON.parse(resp.getContentText());
  } else {
    Logger.log(resp.getResponseCode());
    Logger.log(resp.getContentText());
    return '';
  }
}

function displayResults(results) {
  if (results.length > 0) {
    results.forEach(function(result) {
      Logger.log(JSON.stringify(result));
    });
  } else {
   	Logger.log('No results.');
  }
}


var IQ_API_HOST = 'https://rc-viewiq.channelfactory.com/api/v1/';
var CHANGED_ACCOUNTS = 'pacing_report/flights/campaigns/budgets/updated/';
var SET_CAMPAIGN_ACCOUNT_UPDATE_TIMES = 'pacing_report/status/'

function main() {
  Logger.log('Updating budget allocations...');

  var mcc_account_id = getAccountId();
  var updatedBudgets = getBudgetAllocations(mcc_account_id);
  var accountIds = Object.keys(updatedBudgets.accounts);

  var accountIterator = AdsManagerApp
      .accounts()
      .withIds(accountIds)
  	  .get();

  processAllAccounts(accountIterator, updatedBudgets);

  Logger.log('Update complete.');
}

function processAllAccounts(iterator, updatedBudgets) {
  while (iterator.hasNext()) {
    var account = iterator.next();

    AdsManagerApp.select(account);

    processAccount(updatedBudgets);
  }
}

function getAccountId() {
  return AdsApp.currentAccount().getCustomerId().split('-').join('');
}

function getBudgetAllocations(mcc_account_id) {
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

function processAccount(updatedBudgets) {
  var accountId = getAccountId();
  var campaignBudgets = updatedBudgets.accounts[accountId]

  var videoCampaignIterator = AdsApp.videoCampaigns()
      .withIds(Object.keys(campaignBudgets))
      .get();

  var displayCampaignIterator = AdsApp.campaigns()
      .withIds(Object.keys(campaignBudgets))
      .get();

  processCampaigns(videoCampaignIterator, campaignBudgets);
  processCampaigns(videoCampaignIterator, campaignBudgets);
}

function processCampaigns(iterator, campaignBudgets) {
  var campaignIterator = iterator;

  while (campaignIterator.hasNext()) {
    var campaign = campaignIterator.next();
    var campaignBudget = campaign.getBudget();
    var campaignId = String(campaign.getId());

    var updatedCampaignBudget = campaignBudgets[campaignId];

    if (updatedCampaignBudget) {
      try {
        campaignBudget.setAmount(updatedCampaignBudget);

      } catch(err) {
        campaignBudget.setTotalAmount(updatedCampaignBudget);
      }
    }
  }
}

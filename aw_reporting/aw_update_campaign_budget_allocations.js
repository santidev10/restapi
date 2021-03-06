var IQ_API_HOST = 'https://viewiq.channelfactory.com/api/v1/';
var CHANGED_ACCOUNTS = 'pacing_report/flights/campaigns/budgets/updated/';
var CAMPAIGNS_SYNCED = 'pacing_report/status/'

function main() {
  Logger.log('Updating budget allocations...');

  var mcc_account_id = getAccountId();
  var updatedBudgets = getBudgetAllocations(mcc_account_id);

  if (Object.keys(updatedBudgets) <= 0) {
    Logger.log('No campaign budgets to update.')
    return
  }

  Logger.log(updatedBudgets)

  var accountIds = Object.keys(updatedBudgets);

  var accountIterator = AdsManagerApp
      .accounts()
      .withIds(accountIds)
  	  .get();

  // Only process accounts when not running in preview mode
  if (!AdsApp.getExecutionInfo().isPreview()) {
    processAllAccounts(accountIterator, updatedBudgets);
  }

  var campaignIds = []
  accountIds.forEach(function(accountId) {
  	campaignIds = campaignIds.concat(Object.keys(updatedBudgets[accountId]));
  });

  // Only update sync_times when not running in preview mode
  if (!AdsApp.getExecutionInfo().isPreview()) {
    response = updateSyncTimes(campaignIds);
	  Logger.log(response);
  }
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
  var campaignBudgets = updatedBudgets[accountId]

  var videoCampaignIterator = AdsApp.videoCampaigns()
      .withIds(Object.keys(campaignBudgets))
      .get();

  var displayCampaignIterator = AdsApp.campaigns()
      .withIds(Object.keys(campaignBudgets))
      .get();

  processCampaigns(videoCampaignIterator, campaignBudgets);
  processCampaigns(displayCampaignIterator, campaignBudgets);
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

// Need to get endpoint from backend, get flight campaigns with new budget allocations
/*
1. get camaign ids to edit with their new budgets from backend
2. Retrieve campaigns
3. Iterate through campaigns and set new budget amounts
*/
const IQ_API_HOST = "https://rc.view-iq.channelfactory.com/api/v1/";
const CHANGED_ACCOUNTS = "aw_creation_changed_accounts_list/";
const CODE_ENDPOINT = "aw_creation_code/";
const CHANGES_STATUS_PATH = "aw_creation_changes_status/";

function main() {
    Logger.log('Updating budget allocations...')
    // Object with keys -> campaign id, value -> newBudget
    const mcc_account_id = get_mcc_account_id()
    const campaignsToUpdate = getUpdatedCampaignBudgets(mcc_account_id);

    const campaignIterator = AdsApp.videoCampaigns().withIds(Object.keys(campaignsToUpdate));

    updateCampaignBudgets(campaignIterator)
    Logger.log('Budget allocations update complete')_
}

function get_mcc_account_id(){
    return AdWordsApp.currentAccount().getCustomerId().split('-').join('');
}

function getUpdatedCampaignBudgets() {
    // Retrieve accounts that have been edited
    const options = {
        muteHttpExceptions : true,
        method: "GET",
    };
    const resp = UrlFetchApp.fetch(IQ_API_HOST + CHANGED_ACCOUNTS + '/', options);
    if (resp.getResponseCode() == 200) {
        return JSON.parse(resp.getContentText());
    } else {
        Logger.log(resp.getResponseCode());
        Logger.log(resp.getContentText());
        return '';
    }
}

function updateCampaignBudgets(iterator) {
    campaignIterator = iterator;

    while (campaignIterator.hasNext()) {
        const campaign = campaignIterator.next();
        const budgetObj = campaign.getBudget()
        const newBudget = campaignsToUpdate[campaign['Id']]

        budgetObj.setAmount(newBudget)
    }
}

function getManagedAccounts() {
    return AdsManagerApp.accounts.get();
}

function main() {
    var ids = get_changed_account_ids(get_account_id());
    if(ids.length > 0){
        var accountSelector = MccApp.accounts().withCondition("CustomerId IN [" + ids.join(",") + "]");
        accountSelector.executeInParallel('get_and_exec_changes');
    }
}

function get_and_exec_changes(){
    var account_id = get_account_id();
    Logger.log(account_id);
    var resp = get_code(account_id);
    if(resp){
        eval(resp['code']);
    }
}

function get_account_id(){
  return AdWordsApp.currentAccount().getCustomerId().split('-').join('');
}

var IQ_API_HOST = "https://iq-dev.channelfactory.com/api/v1/";
var CHANGED_ACCOUNTS = "aw_creation_changed_accounts_list/";
var CODE_ENDPOINT = "aw_creation_code/";
var CHANGES_STATUS = "aw_creation_changes_status/";


function get_changed_account_ids(manager_id){
    var options = {
        muteHttpExceptions : true,
        method: "GET",
    };
    var resp = UrlFetchApp.fetch(IQ_API_HOST + CHANGED_ACCOUNTS + manager_id + '/', options);
    if(resp.getResponseCode() == 200) {
        return JSON.parse(resp.getContentText());
    }else{
        Logger.log(resp.getResponseCode());
        Logger.log(resp.getContentText());
        return '';
    }
}

function get_code(account_id){
    var options = {
        muteHttpExceptions : true,
        method: "GET",
    };
    var resp = UrlFetchApp.fetch(IQ_API_HOST + CODE_ENDPOINT + account_id + '/', options);

    if(resp.getResponseCode() == 200) {
        return JSON.parse(resp.getContentText());
    }else{
        Logger.log(resp.getResponseCode());
        Logger.log(resp.getContentText());
        return '';
    }
}
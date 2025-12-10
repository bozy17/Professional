'use strict';
import { ai } from '/Script/Source/Integration2/CustomCloudScripts/CustomCloudScriptApi.js';
var navigator = {};
var window = {};
var rowsProcessed = 0;

function testConnection(context) {
    
    ai.log.logInfo('Starting Test');
    
    // Test the Table Structure API call
    //accessToken = getToken();
    
    var url = 'https://bigquery.googleapis.com/bigquery/v2/projects/as-finance-430214/datasets/gsheets/tables/broker_code_map'; /specific GSheet needs to be updated!!
    var method = 'GET';
    // Setup request in body, sample is using xml, but could be json
    var body = '';
    var headers = null;
 
    // Step 2: Send request and receive response
    var response = null;
    try {
        response = ai.https.authorizedRequest(url, method, body, headers);
    }
    catch (exception) {
        // Example of logging to the CCDS log using the Error loglevel
        ai.log.logError('Test Connection HTTPS Request failed', ''+exception);
        return false;
    }
    // Step 3: Interrogate response to see if it was successful. 
    // Return true or false depending on the result.
    // Check that http communication was successful
    if (response.getHttpCode() == '200') {
        var responseBody = response.getBody();
    	return true;
    } else {
        ai.log.logError('Error Code : ' + response.getHttpCode());
	    return false;
    }
    
}

function importStructure(context) {
    var builder = context.getStructureBuilder();
 
    //Get Structure from api call
    //accessToken = getToken();
    
    var url = 'https://bigquery.googleapis.com/bigquery/v2/projects/as-finance-430214/datasets/gsheets/tables/broker_code_map';
    var method = 'GET';
    // Setup request in body, sample is using xml, but could be json
    var body = '';
    var headers = null;
 
    // Step 2: Send request and receive response
    var response = null;
    try {
        response = ai.https.authorizedRequest(url, method, body, headers);
    }
    catch (exception) {
        // Example of logging to the CCDS log using the Error loglevel
        ai.log.logError('Test Connection HTTPS Request failed', ''+exception);
        return false;
    }
    
    var resBody = JSON.parse(response.getBody());
    var tableName = resBody.tableReference.tableId;
    var fields = [];
    fields = resBody.schema.fields;

    var table = builder.addTable(tableName);
    table.setDisplayName(tableName);
    
    var name = null;
    var type = null;
    
    for (var i = 0; i < fields.length; i++) {
        
        var order = i+1;
        
        addColumn(table, fields[i], i+1)
        
    }
    
}

function addColumn(table, column, index) {
    var recordColumn = table.addColumn(column.name);
    recordColumn = recordColumn.setDisplayName(column.name);
    
    //known GBQ datatypes.
    switch(column.type){
        case "text":recordColumn.setTextColumnType(column.length);break;
        case "int":recordColumn.setIntegerColumnType();break;
        case "float":recordColumn.setFloatColumnType();break;
        case "boolean":recordColumn.setBooleanColumnType();break;
        case "datetime":recordColumn.setDateTimeColumnType();break;
        default:recordColumn.setTextColumnType(200);break;
    }
    recordColumn.setDisplayOrder(index);
}

function importData(context) {
    var rowset = context.getRowset();
    
    ai.log.logInfo('Rowset: ' + rowset)
 
    // Step 1: Get the data via https or a static source
    //accessToken = getToken();
    var url = 'https://bigquery.googleapis.com/bigquery/v2/projects/as-finance-430214/datasets/gsheets/tables/broker_code_map/data?maxResults=1000';
    var method = 'GET';
    // Setup request in body, sample is using xml, but could be json
    var body = '';
    var headers = null;
 
    // Step 2: Send request and receive response
    var response = null;
    try {
        response = ai.https.authorizedRequest(url, method, body, headers);
    }
    catch (exception) {
        // Example of logging to the CCDS log using the Error loglevel
        ai.log.logError('Test Connection HTTPS Request failed', ''+exception);
        return false;
    }
    
    //First call to get pageToken
    var resBody = JSON.parse(response.getBody());
    var pageToken = resBody.pageToken;
    var rows = resBody.rows;
    var totalLength = 0;
    var totalRows = resBody.totalRows;
    var rowsProcessed = 0;
    ai.log.logInfo("Number of Rows in DataSet : " + resBody.totalRows)

    addRecords(rows, rowset);
    
    var count = 1;
    
    //Future calls after pageToken is recieved
    while(rowsProcessed < totalRows) {
        var url = 'https://bigquery.googleapis.com/bigquery/v2/projects/as-finance-430214/datasets/gsheets/tables/broker_code_map/data?maxResults=1000&pageToken=' + pageToken;
        var method = 'GET';
        // Setup request in body, sample is using xml, but could be json
        var body = '';
        var headers = null;
        
        var response = null;
        try {
            response = ai.https.authorizedRequest(url, method, body, headers);
        }
        catch (exception) {
            // Example of logging to the CCDS log using the Error loglevel
            ai.log.logError('Test Connection HTTPS Request failed', ''+exception);
            return false;
        }
        
        count++;
        
        ai.log.logInfo("Count : " + count)
        
        //Calls after first page
        resBody = JSON.parse(response.getBody());
        pageToken = resBody.pageToken;
        var rows = resBody.rows;
        
        addRecords(rows, rowset);
        
        ai.log.logInfo("Rows Processed : " + rowsProcessed)
    
    }
    
    //function to add Records to the table
    function addRecords(rows, rowset) {
    
    for (var i = 0; i < rows.length; i++) {

        var cells = [];
        var row = rows[i].f;
        
        for (var j = 0; j < rowset.getColumns().length; j++) {
            //GBQ provides position-based response values, so we resolve them by using each column's displayOrder created during structure import.
            var order = rowset.getColumns()[j].getDisplayOrder() - 1;
            var type = rowset.getColumns()[j].getColumnType();
            
            var value = row[order].v;
            
            //the reason this evaluation is in place is moderately absurd, but essential nonetheless... https://www.epochconverter.com/
            if (type == 'DateTimeColumn') {
                //take GBQ seconds-based date, and convert to JS-friendly milliseconds. 
                //milliseconds = seconds * 1000
                cells.push(new Date(value * 1000));
            } else {
                cells.push(value);
            }
        }
        rowset.addRow(cells);
        rowsProcessed++;
    }
    
}
    
    
    //var avgLength = totalLength / rows.length;
    
    //ai.log.logInfo("Average Number of Columns : " + avgLength);
    
}

function previewData(context)
{
    // Step 1: Make use of passed in contextual information. Here, we are 
    // simply assigning tableId, maxRows and columnNames to variables to be 
    // used elsewhere in this script.
    var rowset = context.getRowset();
    var tableId = rowset.getTableId();
    var maxRows = rowset.getMaxRows();
    var columnNames = rowset.getColumnNames(); 
 
    // Step 2: Create a https request that will return data - for this example, 
    // we will demonstrate with a json response
    var url = 'https://www.yourapi.com';
    var method = 'POST';
    // Setup request in body, sample is using xml, but could be json
    var body = '<request>yourRequest</request>';
    var headers = { 'Content-Type': 'your-request-type' };
 
    // Step 3: Send https request and receive response. Normally you would 
    // want to check that the response contains a success message first before
    // looking at the rows.
    var response = null;
    try {
        response = ai.https.request(url, method, body, headers);
    }
    catch (exception) {
        // Example of logging to the CCDS log using the Error loglevel
        ai.log.logInfo('Preview Data HTTPS Request failed', ''+exception);
        return false;
    }
 
    // Step 4: Since we're expecting requested JSON as the response format, we 
    // need to parse the https response body into a JSON object for ease of use.
    var rows = JSON.parse(response.getBody());
 
    // Step 5: Process each row to extract the cell values for each column 
    // and add them as an array to the rowset in the expected column order.
    // In this case, each 'row' is represented as a single JSON object, 
    // where each property is the name of the column and each value of each 
    // property is the cell value for the row's column.
    for(var i = 0; i < rows.length; i++) {
          var cells = [];
          var row = rows[i];
          for(var key in row){
             cells.push(row[key]);                 
          }
          rowset.addRow(cells);
    }
}

package com.instashop.cloud.online;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@DefaultCoder(AvroCoder.class)
public class LoyaltyInfo {
  private static final Logger LOG = LoggerFactory.getLogger(LoyaltyInfo.class);
  private String[] fields;
  private static DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
  
  private enum Field {
	LoyaltyJSON,
	//Fields coming from Loyalty Engine Start
    PARTY_ID, 
    LOYALTY_ACCOUNT_ID, 
    GENDER, 
    
    LOYALTY_MEMBER_STATUS, 
    TOTAL_POINT_BALANCE, 
    LOYALTY_MEMBER_CURRENT_TIER,
    
    LOYALTY_MEMBER_PREVIOUS_TIER, 
    LOYALTY_EVENT_TYPE, 
    LOYALTY_EVENT_TIME,
    //Fields coming from Loyalty Engine End
    
    //Fields coming from POSLog JSON Start
    RETAILSTOREID, 
    WORKSTATIONID, 
    BUSINESSDATE, 
    
    OPERATORNAME, 
    ITEMCOUNT, 
    TENDER_TOTAL, 
    
    TENDER_TYPE, 
    TRANSACTION_DISCOUNT_AMOUNT, 
    TAXTOTAL, 
    
    BARCODE,
	//Fields coming from POSLog JSON end
    POSLogJSON;
  }
  
  public List<Map<String, String>> lineItems = null;

 
	
  public LoyaltyInfo() {
    // for Avro
  }

  public static LoyaltyInfo newLoyaltyInfo(String posLogJSON) {
  	LOG.info("Printing line......" + posLogJSON);
  	LoyaltyInfo loyaltyInfoObj = new LoyaltyInfo();
  	try {
  		JSONObject posLogObj = new JSONObject(posLogJSON);
  		JSONObject transactionobject = posLogObj.getJSONObject("Transaction");
  		JSONObject requestObject = transactionobject.getJSONObject("stmt:TransactionExtension").getJSONArray("stmt:Receipt").getJSONObject(1).getJSONObject("receipt").getJSONObject("request");
  	    LOG.info(transactionobject.getString("RetailStoreID"));
      	String[] loyaltyFields = new String[21];
      	
      	loyaltyFields[Field.RETAILSTOREID.ordinal()] = transactionobject.getString("RetailStoreID");
      	loyaltyFields[Field.WORKSTATIONID.ordinal()] = transactionobject.getString("WorkstationID");
      	loyaltyFields[Field.BUSINESSDATE.ordinal()] = transactionobject.getString("BusinessDayDate");
      	
      	loyaltyFields[Field.OPERATORNAME.ordinal()] = transactionobject.getJSONObject("OperatorID").getString("-OperatorName");
      	loyaltyFields[Field.ITEMCOUNT.ordinal()] = transactionobject.getJSONObject("RetailTransaction").getString("ItemCount");
      	loyaltyFields[Field.TENDER_TOTAL.ordinal()] = requestObject.getString("TENDER_TOTAL");
      	
      	loyaltyFields[Field.TENDER_TYPE.ordinal()] = requestObject.getJSONObject("TENDER").getString("TENDER_TYPE");
      	loyaltyFields[Field.TRANSACTION_DISCOUNT_AMOUNT.ordinal()] = requestObject.getJSONObject("TRANSACTION_DISCOUNTS").getJSONObject("DISCOUNT_LINE").getString("TRANSACTION_DISCOUNT_AMOUNT");
      	loyaltyFields[Field.TAXTOTAL.ordinal()] = requestObject.getString("TAXTOTAL");
      	
      	loyaltyFields[Field.BARCODE.ordinal()] = requestObject.getString("BARCODE");
      	loyaltyFields[Field.POSLogJSON.ordinal()] = posLogJSON;
      	
      	loyaltyFields[Field.LoyaltyJSON.ordinal()] = "";
      	
		loyaltyFields[Field.PARTY_ID.ordinal()] = "";
		loyaltyFields[Field.LOYALTY_ACCOUNT_ID.ordinal()] = "";
		loyaltyFields[Field.GENDER.ordinal()] = "";
		
		loyaltyFields[Field.LOYALTY_MEMBER_STATUS.ordinal()] = "";
		loyaltyFields[Field.TOTAL_POINT_BALANCE.ordinal()] = "";
		loyaltyFields[Field.LOYALTY_MEMBER_CURRENT_TIER.ordinal()] = "";
		
		loyaltyFields[Field.LOYALTY_MEMBER_PREVIOUS_TIER.ordinal()] ="";
		loyaltyFields[Field.LOYALTY_EVENT_TYPE.ordinal()] = "";
		loyaltyFields[Field.LOYALTY_EVENT_TIME.ordinal()] = "";
	    
	    
      	loyaltyInfoObj.fields = loyaltyFields;
      	
      	List<Map<String, String>> lineItemsList = new ArrayList<Map<String, String>>();
      	JSONArray lineitems_json = transactionobject.getJSONObject("RetailTransaction").getJSONArray("LineItem");
      	
      	for (int i = 0; i < lineitems_json.length(); i++) {
      		JSONObject lineitem_json = lineitems_json.getJSONObject(i);
      		try {
	      		Map<String, String> lineitemMap = new HashMap<String, String>();
	          	lineitemMap.put("sequence_number", lineitem_json.getString("SequenceNumber"));
	          	lineitemMap.put("item_type", lineitem_json.getJSONObject("Sale").getString("-ItemType"));
	          	lineitemMap.put("unit_list_price", lineitem_json.getJSONObject("Sale").getString("UnitListPrice"));
	          	lineitemMap.put("regular_sales_unit_price", lineitem_json.getJSONObject("Sale").getString("ActualSalesUnitPrice"));
	          	lineitemMap.put("extended_discount_amount", lineitem_json.getJSONObject("Sale").getString("ExtendedDiscountAmount"));
	          	lineitemMap.put("quantity", lineitem_json.getJSONObject("Sale").getString("Quantity"));
	          	
	          	lineItemsList.add(lineitemMap);
      		} catch (Exception e) {
      			LOG.info("Error Ignored");
      		}
        }
      	loyaltyInfoObj.lineItems = lineItemsList;
      	
  	} catch (Exception e) {
  		LOG.info("Error happened.......");
  		LOG.info(e.toString());
  	}
  	 
  	//LOG.info(line);
    
    return loyaltyInfoObj;
  }
  
  public static LoyaltyInfo setAdditionalLoyaltyInfo(LoyaltyInfo loyaltyInfoObj, String LoyaltyEngineResponse) {
	  	//LOG.info("Printing line......" + posLogJSON);
	  	
  	try {
  		String[] loyaltyFields = loyaltyInfoObj.getFields();
  	    
  	    JSONObject loyaltyObj = new JSONObject(LoyaltyEngineResponse);
		JSONObject loyaltyobject = loyaltyObj.getJSONObject("Loyalty");
		LOG.info(loyaltyobject.getString("party_id"));
    
		loyaltyFields[Field.LoyaltyJSON.ordinal()] = LoyaltyEngineResponse;
		loyaltyFields[Field.PARTY_ID.ordinal()] = loyaltyobject.getString("party_id");
		loyaltyFields[Field.LOYALTY_ACCOUNT_ID.ordinal()] = loyaltyobject.getString("loyalty_acct_id");
		loyaltyFields[Field.GENDER.ordinal()] = loyaltyobject.getString("gender");
		loyaltyFields[Field.LOYALTY_MEMBER_STATUS.ordinal()] = loyaltyobject.getString("loyalty_member_status");
		loyaltyFields[Field.LOYALTY_MEMBER_CURRENT_TIER.ordinal()] = loyaltyobject.getString("loyalty_member_current_tier");
		loyaltyFields[Field.LOYALTY_MEMBER_PREVIOUS_TIER.ordinal()] = loyaltyobject.getString("loyalty_member_previous_tier");
		loyaltyFields[Field.LOYALTY_EVENT_TYPE.ordinal()] = loyaltyobject.getString("loyalty_event_type");
		loyaltyFields[Field.LOYALTY_EVENT_TIME.ordinal()] = LocalDateTime.now().format(dateTimeFormatter);
	    
	    
		loyaltyInfoObj.fields = loyaltyFields;
  	} catch (Exception e) {
  		LOG.info(e.toString());
  	}
  	 
  	//LOG.info(line);
    
    return loyaltyInfoObj;
  }

  public String getRetailStoreID() {
    return fields[Field.RETAILSTOREID.ordinal()];
  }

  public String getWorkstationID() {
	 return fields[Field.WORKSTATIONID.ordinal()];
  }

  public String getBusinessDayDate() {
	 return fields[Field.BUSINESSDATE.ordinal()];
  }
  
  public String getOperatorName() {
	 return fields[Field.OPERATORNAME.ordinal()];
  }
  
  public String getItemCount() {
	 return fields[Field.ITEMCOUNT.ordinal()];
  }
  
  public String getTender_Total() {
	 return fields[Field.TENDER_TOTAL.ordinal()];
  }
  
  public String getTender_type() {
	 return fields[Field.TENDER_TYPE.ordinal()];
  }
  
  public String getTransaction_Discount_Amount() {
	 return fields[Field.TRANSACTION_DISCOUNT_AMOUNT.ordinal()];
  }
  
  public String getTaxTotal() {
	 return fields[Field.TAXTOTAL.ordinal()];
  }
  
  public String getBarcode() {
	 return fields[Field.BARCODE.ordinal()];
  }
  
  public String getLoyalty_acct_id() {
	 return fields[Field.LOYALTY_ACCOUNT_ID.ordinal()];
  }
  
  public String getGender() {
	 return fields[Field.GENDER.ordinal()];
  }
  
  public String getLoyalty_member_status() {
	 return fields[Field.LOYALTY_MEMBER_STATUS.ordinal()];
  }
  
  public String getLoyalty_member_current_tier() {
	 return fields[Field.LOYALTY_MEMBER_CURRENT_TIER.ordinal()];
  }
  
  public String getLoyalty_member_previous_tier() {
	 return fields[Field.LOYALTY_MEMBER_PREVIOUS_TIER.ordinal()];
  }
  
  public String getLoyalty_event_type() {
	 return fields[Field.LOYALTY_EVENT_TYPE.ordinal()];
  }
  
  public String getLoyaltyJSON() {
	 return fields[Field.LoyaltyJSON.ordinal()];
  }
  
  public String getLoyalty_event_time() {
	 return fields[Field.LOYALTY_EVENT_TIME.ordinal()];
  }
  
  public String getPOSLogJSON() {
	 return fields[Field.POSLogJSON.ordinal()];
  }
  
  public String getParty_id() {
	 return fields[Field.PARTY_ID.ordinal()];
  }
  
  public String[] getFields() {
	 return this.fields;
  }
}

package main.java.com.iisi.corebanking.data.migration;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class FunctionKeyApp {
	private final Properties settings;
	private final Properties settingTOA;
	private final Properties settingCS;
	private final char FIELD_DELIMITER;
	private final String lineSeperator;
	private List<String> lineList;
	private final String migrationDate = "20150911";
	
	/**
	 * Create an entity of Information for Function Key Application.
	 * @param settings
	 *            - The each module the criteria Setting.
	 * @param delimiter
	 *            -  Define the Delimiter "|".
	 * @param lineSeperator
	 *            -  Define the newline character.
	 * @param settingTOA
	 *            - Loading the Internal Account List.
	 * @param settingCS
	 *            - Loading the Clearing Suspence Internal Account List.
	 */
	public FunctionKeyApp(Properties settings, char delimiter, String lineSeperator, Properties settingTOA, Properties settingCS){
		this.settings = settings;
		this.settingTOA = settingTOA;
		this.settingCS = settingCS;
		this.FIELD_DELIMITER = delimiter;
		this.lineSeperator = lineSeperator;
	}
	
	/**
	 * Initialize the One Record Separate to Mapping.
	 * 
	 * @param line
	 *            - One Record String.
	 */
	public void fn_setTheLineList(String line){
		this.lineList = lineSeparete(line);
	}
	
	/**
	 * Initialize the One Record Separate to Mapping.
	 * @param line
	 *            - One Record String.
	 * @return lineSeparate(line);
	 *            - The List of one Record Mapping.
	 */
	public List<String> fn_newTheLineList(String line){
		return lineSeparete(line);
	}
	
	/**
	 * Insert the original value to specify position.
	 * 
	 * @param configSingleValue
	 *            - One Record String.
	 * @return onlyInsertField
	 *            - Return the Value by one Record.
	 */
	public String fn_onlyInsertOriginal(String configSingleValue){
		String onlyInsertField = this.lineList.get(Integer.parseInt(configSingleValue)).trim();
		return onlyInsertField;
	}
	
	/**
	 * Insert the Company Code that by format from Account or other reference.
	 * 
	 * @param configSingleValue
	 *            - One Record String.
	 * @param uploadCompanyCode
	 *            - Company material.
	 * @return acCompanyCodeString;
	 *            - Return the Company.Code.
	 */
	public String fn_acCompanyCode(String configSingleValue, String uploadCompanyCode){
		String acCompanyCodeString ="";
		if (!settings.getProperty("fn", "").trim().equals("GL")) {
		acCompanyCodeString = uploadCompanyCode+getIndexOrValue(configSingleValue, this.lineList).substring(0,3);
		} else {
		//Internal Account
		String company = getIndexOrValue(configSingleValue, this.lineList);
		acCompanyCodeString = uploadCompanyCode+company.substring(company.length()-3,company.length());
		}
		return acCompanyCodeString;
	}
	
	/**
	 * Insert the Other dynamic state Value. e.g. Date or Blank.
	 * 
	 * @param configSingleValue
	 *            - One Record String.
	 * @return insertOtherString;
	 *            - Return the Company.Code.
	 */
	public String fn_insertOther(String configSingleValue){
		String insertOtherString = getIndexOrValue(configSingleValue, null);
		if (insertOtherString.equalsIgnoreCase("DATE")) {
			//insertOtherString = new SimpleDateFormat("yyyyMMdd").format(new Date());
			insertOtherString = migrationDate;
		} else if (insertOtherString.equalsIgnoreCase("BLANK")) {
			insertOtherString = "";
			
		}
		return insertOtherString;
	}
	
	/**
	 * Insert the Cheque Clearing Suspence base on the Account.
	 * 
	 * @param configSingleValue
	 *            - One Record String.
	 * @param uploadCompanyCode
	 *            - Company material.
	 * @return csString;
	 *            - Return Cheque Clearing Suspence Internal Account.
	 */
	public String fn_clearingSuspense(String configSingleValue, String uploadCompanyCode){
		String csString = uploadCompanyCode+getIndexOrValue(configSingleValue, this.lineList).substring(0,3)+this.lineList.get(Integer.parseInt(settings.getProperty("CY")));
		csString = settingCS.getProperty(csString);
		return csString;
	}
	
	/**
	 * Insert the Internal Account base on the Amount greater or less Zero,
	 * Reference by Account or others.
	 * 
	 * @param configSingleValue
	 *            - One Record String.
	 * @param uploadCompanyCode
	 *            - Company material.
	 * @return toaJudgeString;
	 *            - Return the Value of Judge.
	 */
	public String fn_TOAJudge(String configSingleValue, String uploadCompanyCode){
		String[] putArray = getIndexOrValue(configSingleValue, null).split(",");
		
		//BigDecimal availableBalance = getBigDecimal((this.lineList.get(Integer.parseInt(putArray[0])).trim()));
		BigDecimal availableBalance = new BigDecimal(this.lineList.get(Integer.parseInt(putArray[0])).trim());
		//BigDecimal zero = getBigDecimal("0");
		BigDecimal zero = new BigDecimal("0");
		
		String toaJudgeString = "";
		String lessMsg="";
		String moreAndEqualMsg="";
		
		String toaConditionString = uploadCompanyCode;
		String currencyCodeString = this.lineList.get(Integer.parseInt(settings.getProperty("CY")));
		
		if (putArray[2].trim().equalsIgnoreCase("TOA")) {
			lessMsg = this.lineList.get(Integer.parseInt(putArray[1]));
			String branchCodeString = "";
			
			if (!settings.getProperty("fn", "").trim().equals("GL")) {
				branchCodeString = lessMsg.substring(0,3);
			} else {
				branchCodeString = lessMsg.length() > 8 ? lessMsg.substring(lessMsg.length()-3,lessMsg.length()) : this.lineList.get(5);
			}
			
			String toaCondition = toaConditionString+branchCodeString+currencyCodeString;
			moreAndEqualMsg = getTOAString(toaCondition);
			
		} else if (putArray[1].trim().equalsIgnoreCase("TOA")) {
			moreAndEqualMsg = this.lineList.get(Integer.parseInt(putArray[2]));
			String branchCodeString = "";
			if (!settings.getProperty("fn", "").trim().equals("GL")) {
				branchCodeString = moreAndEqualMsg.substring(0,3);
			} else {
				branchCodeString = moreAndEqualMsg.length() > 8 ? moreAndEqualMsg.substring(moreAndEqualMsg.length()-3,moreAndEqualMsg.length()) : this.lineList.get(5);
			}
			
			String toaCondition = toaConditionString+branchCodeString+currencyCodeString;
			lessMsg = getTOAString(toaCondition);
		}
		
		if (availableBalance.compareTo(zero) < 0){
			toaJudgeString = lessMsg;
		} else {
			toaJudgeString = moreAndEqualMsg;
		}
		return toaJudgeString;
	}
	
	/**
	 * Reformat the numerical to two decimal.
	 * 
	 * @param configSingleValue
	 *            - One Record String.
	 * @return negateString;
	 *            - Return the numerical is formated.
	 */
	public String fn_negateValue(String configSingleValue){
		String negateString = getBigDecimal(getIndexOrValue(configSingleValue, this.lineList)).toString();//.negate().toString();
		return negateString;
	}
	
	/**
	 * Calculate the Two value and rounding to two decimal.
	 * 
	 * @param configSingleValue
	 *            - One Record String.
	 * @return combineString;
	 *            - Return total amount.
	 */
	public String fn_combine2FieldValue(String configSingleValue){
		String[] putArray = getIndexOrValue(configSingleValue, null).split(",");
		//BigDecimal availableBalance = getBigDecimal(this.lineList.get(Integer.parseInt(putArray[0].trim())));
		BigDecimal availableBalance = new BigDecimal(this.lineList.get(Integer.parseInt(putArray[0].trim())));
		//BigDecimal freezeAmount = getBigDecimal(this.lineList.get(Integer.parseInt(putArray[1].trim())));
		BigDecimal freezeAmount = new BigDecimal(this.lineList.get(Integer.parseInt(putArray[1].trim())));
		
		String currencyCodeString = this.lineList.get(Integer.parseInt(settings.getProperty("CY", "")));//For internal Account of JPY
		
		String combineString = !currencyCodeString.equals("JPY") 
				? 
				getBigDecimal(availableBalance.add(freezeAmount).toString()).toString() 
				: 
				getBigDecimalNoPoint(availableBalance.add(freezeAmount).toString()).toString();
		
		return combineString;
	}
	
	/**
	 * Insert the Internal Account base on account or other reference.
	 * 
	 * @param configSingleValue
	 *            - One Record String.
	 * @return toaString;
	 *            - Return Internal Account.
	 */
	public String fn_onlyTOAInsert(String configSingleValue, String uploadCompanyCode){
		String toaConditionString = uploadCompanyCode;
		String branchCodeString = getIndexOrValue(configSingleValue, this.lineList).substring(0,3);
		String currencyCodeString = this.lineList.get(Integer.parseInt(settings.getProperty("CY")));
		String toaCondition = toaConditionString+branchCodeString+currencyCodeString;
		String toaString = getTOAString(toaCondition);
		return toaString;
	}
	
	/**
	 * Insert the Internal Account base on account or other reference with Currency.
	 * 
	 * @param configSingleValue
	 *            - One Record String.
	 * @return toaString;
	 *            - Return Internal Account.
	 */
	public String fn_branchTOAInsert(String configSingleValue){
		String branchCodeString = getIndexOrValue(configSingleValue, this.lineList);
		String currencyCodeString = this.lineList.get(Integer.parseInt(settings.getProperty("CY")));
		String toaCondition = branchCodeString+currencyCodeString;
		String toaString = getTOAString(toaCondition);
		return toaString;
	}
	
	String Limit_formatLine_Parent="";
	String Limit_formatLine_Child="";
	Set<String> ListMap = new HashSet<String>();
	HashMap<String, String> mapValue = new HashMap<String, String>();
	
	/**
	 * Base on the fn_limitZero to format LIMIT.
	 * 
	 * @param configSingleValue
	 *            - One Record String.
	 * @return Limit_formatLine_Parent +lineSeperator+ Limit_formatLine_Child;
	 *            - Return Limit data file.
	 */
	public String fn_getLimitZero(){
		for(String keyValue : ListMap){
			Limit_formatLine_Parent += mapValue.get(keyValue) +lineSeperator;
		}
		return Limit_formatLine_Parent +lineSeperator+ Limit_formatLine_Child;
	}
	
	/**
	 * Customization the LIMIT format.
	 * 
	 * @param configSingleValue
	 *            - One Record String.
	 */
	public String fn_limitZero(String configSingleValue) {
		String[] putArray = getIndexOrValue(configSingleValue, null).split(",");
		String limitRef= fn_onlyInsertOriginal(putArray[0]);

		String getContrastValue = putArray[2].trim();
		int indexStart = limitRef.lastIndexOf(putArray[1].trim()) - getContrastValue.length();
		int indexEnd = indexStart + getContrastValue.length();
		String getValue = limitRef.substring(indexStart, indexEnd);
		if (getValue.equals(getContrastValue)) {
			for (int i = 0 ; i < lineList.size() ; i++) {
				if (i != 0) {
					Limit_formatLine_Parent += FIELD_DELIMITER;
				}
				Limit_formatLine_Parent += lineList.get(i);
			}
			Limit_formatLine_Parent += lineSeperator;
		} else {
				for (int i = 0 ; i < lineList.size() ; i++) {
					if (i != 0) {
						Limit_formatLine_Child += FIELD_DELIMITER;
					}
					Limit_formatLine_Child += lineList.get(i);
				}
				Limit_formatLine_Child += lineSeperator;
			
			String key = limitRef.substring(0, limitRef.lastIndexOf(".") 
					- getContrastValue.length())
					+ getContrastValue
					+ limitRef.substring(limitRef.lastIndexOf("."));
			
			String keyValue = "";
			if (ListMap.contains(key)) {
				String oldValue = mapValue.get(key);
				List<String> oldlineList = fn_newTheLineList(oldValue);
				// new lineList
				for (int i = 0 ; i < oldlineList.size() ; i++){
					if (i == 5 || i == 6 || i == 10 ){
						int oldDate = Integer.parseInt(oldlineList.get(i));
						int newDate = Integer.parseInt(lineList.get(i));
						if(oldDate < newDate){
							lineList.set(i, Integer.toString(oldDate)) ;
						}
						continue;
					}
					
					if (i == 8 || i == 11){
						int oldDate = Integer.parseInt(oldlineList.get(i));
						int newDate = Integer.parseInt(lineList.get(i));
						if(oldDate > newDate){
							lineList.set(i, Integer.toString(oldDate)) ;
						}
						continue;
					}
					
					if (i == 16 || i == 25 || i == 27) {
						//BigDecimal oldAmount = getBigDecimal(oldlineList.get(i));
						BigDecimal oldAmount = new BigDecimal(oldlineList.get(i));
						//BigDecimal newAmount = getBigDecimal(lineList.get(i));
						BigDecimal newAmount = new BigDecimal(lineList.get(i));
						
						lineList.set(i, getBigDecimal(oldAmount.add(newAmount).toString()).toString()) ;
					}
				}
				for (int i = 0 ; i < oldlineList.size() ; i++) {
					if (i != 0) {
						keyValue += FIELD_DELIMITER;
					}
					if ( i != Integer.parseInt(putArray[0])) {
						keyValue += lineList.get(i);
					} else {
						keyValue += key;
					}
				}
				mapValue.put(key, keyValue);
			} else {
				ListMap.add(key);
				for (int i = 0 ; i < lineList.size() ; i++) {
					if (i != 0) {
						keyValue += FIELD_DELIMITER;
					}
					if ( i != Integer.parseInt(putArray[0])) {
						keyValue += lineList.get(i);
					} else {
						keyValue += key;
					}
				}
				mapValue.put(key, keyValue);
			}
		}
		return "";
	}
	
	StringBuilder customer = new StringBuilder();
	/**
	 * After fn_CustomerFormat to format CUSTOMER, output the data file.
	 * 
	 * @return customer.toString();;
	 *            - Return CUSTOMER data file.
	 */
	public String fn_getEmailFormat() {
		return customer.toString();
	}
	
	/**
	 * Customization the CUSTOMER format.
	 * 
	 * @param configSingleValue
	 *            - One Record String.
	 * @return Blank
	 */
	public String fn_CustomerFormat(String configSingleValue) {
		String replaceCondition = getIndexOrValue("A", null).trim();
		if (!replaceCondition.equals("")){
			fn_Replace(replaceCondition);
		}

		for (int i = 0 ; i < lineList.size() ; i++) {
			if (i != 0) {
				customer.append(FIELD_DELIMITER);
			}
			customer.append(lineList.get(i));
		}
		customer.append(lineSeperator);
		return "";
	}
	
	int ix =1;
	/**
	 * Base on the fn_GroupFormat to format Group.
	 * 
	 * @return group.toString();;
	 *            - Return Format Sting for group.
	 */
	StringBuilder group = new StringBuilder();
	public String fn_getGroupFormat() {
		return group.toString();
	}
	
	/**
	 * Base on the quantity of value, format other field to use the same value duplicate.
	 * 
	 * @param configSingleValue
	 *            - One Record String.
	 * @return Blank
	 */
	public String fn_GroupFormat(String configSingleValue) {
		String[] putArray = getIndexOrValue(configSingleValue, null).split(",");
		String getContrastValue = "";
		String[] subSeparateArray;
		String newValue = "";
		for (int pair = 1 ; pair < putArray.length ; pair += 2){
			int amountCut = lineList.get(Integer.parseInt(putArray[pair].trim())).split(putArray[0]).length;
			subSeparateArray = getIndexOrValue(configSingleValue+putArray[pair+1].trim(), null).split(",");
			getContrastValue = lineList.get(Integer.parseInt(putArray[pair+1].trim()));
			if (subSeparateArray[0] != "" && getContrastValue.length() > Integer.parseInt(subSeparateArray[1].trim())){
				getContrastValue = handleMultiAndSubField(getContrastValue, subSeparateArray[0], Integer.parseInt(subSeparateArray[1].trim()));
			}
			
			newValue += getContrastValue;
			if (amountCut > 1 && !getContrastValue.equals("")) {
				for (int plus = 1 ; plus < amountCut ; plus++) {
					newValue += putArray[0];
					newValue += getContrastValue;
				}
			}
			lineList.set(Integer.parseInt(putArray[pair+1].trim()), newValue);
			
			newValue = "";
			getContrastValue = "";
		}
		
		String replaceCondition = getIndexOrValue("A", null).trim();
		if (!replaceCondition.equals("")){
			fn_Replace(replaceCondition);
		}
		
		
		for (int i = 0 ; i < lineList.size() ; i++) {
			if (i != 0) {
				group.append(FIELD_DELIMITER);
			}
			group.append(lineList.get(i));
		}
		group.append(lineSeperator);
		return "";
	}
	
	/**
	 * After Customization the IB.Pay.Account format, , output the data file.
	 * 
	 * @return groupIB
	 *            - Return the IB.Pay.Account Data file.
	 */
	StringBuilder groupIB = new StringBuilder();
	public String fn_getIBFormat() {
		return groupIB.toString();
	}
	
	/**
	 * Customization the IB.Pay.Account format.
	 * 
	 * @param configSingleValue
	 *            - One Record String.
	 * @return ""
	 */
	public String fn_IBFormat(String configSingleValue) {
		String[] putArray = getIndexOrValue(configSingleValue, null).split(",");
		String getContrastValue = "";
		String newValue = "";
		for (int pair = 1 ; pair < putArray.length ; pair += 2){
			String[] stringCut = lineList.get(Integer.parseInt(putArray[pair].trim())).split(putArray[0]);
			int amountCut = stringCut.length;
			getContrastValue = lineList.get(Integer.parseInt(putArray[pair+1].trim()));
			
			if (amountCut >= 1 && !getContrastValue.equals("")) {
				for (int plus = 0 ; plus < amountCut ; plus++) {
					if (plus != 0) {
						newValue += putArray[0];
					}
					if ( stringCut[plus].indexOf("FUND.TRANSFER") != -1 || stringCut[plus].indexOf("BILL.PAYMENT") != -1 ) {
						newValue += getContrastValue;
					}
				}
				
				boolean check = true ;
				while(check){
					if (newValue.substring(newValue.length()-1, newValue.length()).equals(":")){
						newValue = newValue.substring(0, newValue.length()-1);
					} else {
						check = false;
					}
				}
			}
			lineList.set(Integer.parseInt(putArray[pair+1].trim()), newValue);
			newValue = "";
			getContrastValue = "";
		}
		
		for (int i = 0 ; i < lineList.size() ; i++) {
			if (i != 0) {
				groupIB.append(FIELD_DELIMITER);
			}
			groupIB.append(lineList.get(i));
		}
		groupIB.append(lineSeperator);
		return "";
	}
	
	/**
	 * After Customization the AA.Deposit format, output the data file.
	 * 
	 * @return aaDeposit
	 *            - Output the AA.Deposit data file.
	 */
	StringBuilder aaDeposit = new StringBuilder();
	public String fn_getaaDeposit() {
		return aaDeposit.toString();
	}
	
	/**
	 * Customization the AA.Deposit format.
	 * 
	 * @param configSingleValue
	 *            - One Record String.
	 * @return ""
	 */
	public String fn_aaDeposit(String configSingleValue) {
		String allField = lineList.get(21);
		String allValue = lineList.get(22);
		String customerField = allField.split("::")[0];
		String customerValue = allValue.split("::")[0];
		//String[] customerFieldNameArray = customerField.split("!!");
		String[] customerFieldValueArray = customerValue.split("!!");
		String fullName = customerFieldValueArray[customerFieldValueArray.length-1];
		
		if (fullName.length() > 65) {
		String newValueString = handleMultiAndSubField(fullName, "!!", 65);
		int countValue = newValueString.split("!!").length;
		String newFieldString = "";
		for (int count = 0 ; count < countValue ; count ++) {
			newFieldString += "!!";
			int counts = count + 1 ;
			newFieldString += "L.DEP.FL.NM:"+counts+":1" ;
		}
		String newCustomerField = customerField.replace("!!L.DEP.FL.NM:1:1", newFieldString);
		String newCustomerValue = customerValue.replace(fullName, newValueString);
		lineList.set(21, allField.replace(customerField, newCustomerField));
		lineList.set(22, allValue.replace(customerValue, newCustomerValue));
		}
		for (int i = 0 ; i < lineList.size() ; i++) {
			if (i != 0) {
				aaDeposit.append(FIELD_DELIMITER);
			}
			aaDeposit.append(lineList.get(i));
		}
		aaDeposit.append(lineSeperator);
		
		return "";
	}
	
	/**
	 * The function for replace the specify character.
	 * 
	 * @return stringByReplace
	 *                  - The format string. 
	 */
	StringBuilder stringByReplace = new StringBuilder();
	public String fn_getReplaceInterface() {
		return stringByReplace.toString();
	}
	
	
	/**
	 * TReplace the specify character DAO.
	 * 
	 * @return Blank
	 */
	public String fn_ReplaceInterface() {
		String replaceCondition = getIndexOrValue("A", null).trim();
		if (!replaceCondition.equals("")) {
			fn_Replace(replaceCondition);
		}
		
		for (int i = 0 ; i < lineList.size() ; i++) {
			if (i != 0) {
				stringByReplace.append(FIELD_DELIMITER);
			}
			stringByReplace.append(lineList.get(i));
		}
		stringByReplace.append(lineSeperator);
		
		return "";
	}
	
	
	/**
	 * Replace the specify character DAO.
	 * 
	 * @return newContent.toString()
	 *               - The format String.
	 */
	public static String areplace(String str, String patten,
			String replacement, int pos) {
		int len = str.length();
		int plen = patten.length();
		StringBuilder newContent = new StringBuilder(len);

		int lastPos = 0;

		do {
			newContent.append(str, lastPos, pos);
			newContent.append(replacement);
			lastPos = pos + plen;
			pos = str.indexOf(patten, lastPos);
		} while (pos > 0);
		newContent.append(str, lastPos, len);
		return newContent.toString();
	}
	
	/**
	 * Replace the specify character DAO.
	 * 
	 * @param configSingleValue
	 *            - One Record String.
	 */
	private void fn_Replace(String replaceCondition){
		String[] replaceConditionArray = replaceCondition.split("\\|");
		for (int i = 0 ; i < replaceConditionArray.length ; i+=3) {
			int whichField = Integer.parseInt(replaceConditionArray[i].trim()); 
			String oldString = replaceConditionArray[i+1].trim();
			String newString = replaceConditionArray[i+2].trim().equals("BLANK") ? " " : replaceConditionArray[i+2].trim();
			lineList.set(whichField, lineList.get(whichField).replace(oldString, newString));
		}		
	}
	
	/**
	 * Mapping the Internal Account.
	 * 
	 * @param toaStringKey
	 *            - Reference String.
	 * @return toaStringKey
	 *            - return the Internal Account.
	 */
	private String getTOAString (String toaStringKey){
		toaStringKey = settingTOA.getProperty(toaStringKey);
		return toaStringKey;
	}
	
	/**
	 * Subroutin for get the value form property. 
	 * 
	 * @param configSingleValue
	 *            - Reference String.
	 * @param lineList
	 *            - Reference List.
	 * @return configFieldValue
	 *            - return property Value.
	 */
	private String getIndexOrValue (String configSingleValue, List<String> lineList){
		String configFieldValue;
		configFieldValue=settings.getProperty(configSingleValue, "").trim();
		
		if(lineList != null){
			configFieldValue = lineList.get(Integer.parseInt(configFieldValue)).trim();
		}
		return configFieldValue;
	}

	/**
	 * Format to Two decimal. 
	 * 
	 * @param moneyInput
	 *            - total amount.
	 * @return moneyOutput
	 *            - return the format value.
	 */
	private BigDecimal getBigDecimal(String moneyInput){
		BigDecimal moneyOutput = new BigDecimal (moneyInput).setScale(2, BigDecimal.ROUND_HALF_UP);//
		return moneyOutput;
	}
	
	/**
	 * Format to Zero decimal. 
	 * 
	 * @param moneyInput
	 *            - total amount.
	 * @return moneyOutput
	 *            - return the format value.
	 */
	private BigDecimal getBigDecimalNoPoint(String moneyInput){
		BigDecimal moneyOutput = new BigDecimal (moneyInput).setScale(0,BigDecimal.ROUND_HALF_UP);
		return moneyOutput;
	}

	/**
	 * Subroutine for separate the line record. 
	 * 
	 * @param line
	 *            - One Record String.
	 * @return list
	 *            - return field List.
	 */
	private List<String> lineSeparete(String line) {
		List<String> list = new ArrayList<String>();
		list.add(line.substring(0, line.indexOf(FIELD_DELIMITER)));
		int start = 0;
		int end = 0;
		while (end != line.length()) {
			start = line.indexOf(FIELD_DELIMITER, end);
			end = line.indexOf(FIELD_DELIMITER, start + 1);
			if (end < 0) {
				end = line.length();
			}
			String sub = line.substring(start + 1, end);
			list.add(sub);
		}
		return list;
	}
	
	/**
	 * Subroutine for insert the delimiter base on sub Value or multi value and field length. 
	 * 
	 * @param currentField
	 *            - the value 
	 * @param delimiter
	 *            - Define the delimiter.
	 * @param cutLengthInt
	 *            - Define the length.
	 * @return processedFieldData
	 *            - return format String List.
	 */
	private String handleMultiAndSubField(String currentField, String delimiter, int cutLengthInt) {
		String cutData = "";
		StringBuilder processedFieldData = new StringBuilder();
		StringBuilder empty = new StringBuilder();
		for (int i = 0; i * cutLengthInt < currentField.length(); i++) {
			if (i != 0) {
				processedFieldData.append(delimiter);
			}
			int startIdx = i * cutLengthInt;
			int endIdx = startIdx + cutLengthInt > currentField.length() ? currentField.length() : startIdx + cutLengthInt;
			cutData = currentField.substring(startIdx, endIdx);
			if ((cutData.substring(0,1).equals("/") || cutData.substring(0,1).equals("-")) && i != 0) {
				cutLengthInt--;
				i = -1;
				processedFieldData = empty;
			} else {
				processedFieldData.append(cutData);
			}
		}
		return processedFieldData.toString();
	}
}

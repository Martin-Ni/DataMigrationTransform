package main.java.com.iisi.corebanking.data.migration;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
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
	private  List<String> lineList;
	
	public FunctionKeyApp(Properties settings, char delimiter, String lineSeperator,  Properties settingTOA, Properties settingCS){
		this.settings = settings;
		this.settingTOA = settingTOA;
		this.settingCS = settingCS;
		this.FIELD_DELIMITER = delimiter;
		this.lineSeperator = lineSeperator;
	}
	
	public void fn_setTheLineList(String line){
		this.lineList = lineSeparete(line);
	}
	public List<String> fn_newTheLineList(String line){
		return lineSeparete(line);
	}
	
	
	public String fn_onlyInsertOriginal(String configSingleValue){
		String onlyInsertField = this.lineList.get(Integer.parseInt(configSingleValue));
		return onlyInsertField;
	}
	
	public String fn_acCompanyCode(String configSingleValue, String uploadCompanyCode){
		String acCompanyCodeString = uploadCompanyCode+getIndexOrValue(configSingleValue, this.lineList).substring(0,3);
		return acCompanyCodeString;
	}
	
	public String fn_insertOther(String configSingleValue){
		String insertOtherString = getIndexOrValue(configSingleValue, null);
		if (insertOtherString.equalsIgnoreCase("DATE")) {
			insertOtherString = new SimpleDateFormat("yyyyMMdd").format(new Date());
			
		} else if (insertOtherString.equalsIgnoreCase("BLANK")) {
			insertOtherString = "";
			
		}
		return insertOtherString;
	}
	
	public String fn_clearingSuspense(String configSingleValue, String uploadCompanyCode){
		String csString = uploadCompanyCode+getIndexOrValue(configSingleValue, this.lineList).substring(0,3)+this.lineList.get(Integer.parseInt(settings.getProperty("CY")));
		csString = settingCS.getProperty(csString);
		return csString;
	}
	
	public String fn_TOAJudge(String configSingleValue, String uploadCompanyCode){

		String[] putArray = getIndexOrValue(configSingleValue, null).split(",");
		BigDecimal availableBalance = getBigDecimal((this.lineList.get(Integer.parseInt(putArray[0])).trim()));
		BigDecimal zero = getBigDecimal("0");
		
		String toaJudgeString = "";
		String lessMsg="";
		String moreAndEqualMsg="";
		
		if(putArray[2].trim().equalsIgnoreCase("TOA")){
			lessMsg = this.lineList.get(Integer.parseInt(putArray[1]));
			moreAndEqualMsg = getTOAString(lessMsg, uploadCompanyCode);
		}else if (putArray[1].trim().equalsIgnoreCase("TOA")){
			moreAndEqualMsg = this.lineList.get(Integer.parseInt(putArray[2]));
			lessMsg = getTOAString(moreAndEqualMsg, uploadCompanyCode);
		}
		
		if (availableBalance.compareTo(zero) < 0){
			toaJudgeString = lessMsg;
		} else {
			toaJudgeString = moreAndEqualMsg;
		}
		return toaJudgeString;
	}
	
	public String fn_negateValue(String configSingleValue){
		String negateString = getBigDecimal(getIndexOrValue(configSingleValue, this.lineList)).negate().toString();
		return negateString;
	}
	
	public String fn_combine2FieldValue(String configSingleValue){
		String[] putArray = getIndexOrValue(configSingleValue, null).split(",");
		BigDecimal availableBalance = getBigDecimal(this.lineList.get(Integer.parseInt(putArray[0].trim())));
		BigDecimal freezeAmount = getBigDecimal(this.lineList.get(Integer.parseInt(putArray[1].trim())));
		String combineString = availableBalance.add(freezeAmount).toString();
		return combineString;
	}
	
	public String fn_onlyTOAInsert(String configSingleValue, String uploadCompanyCode){
		String toaString = getTOAString(getIndexOrValue(configSingleValue, this.lineList), uploadCompanyCode);
		return toaString;
	}
	
	String Limit_formatLine_Parent="";
	String Limit_formatLine_Child="";
	Set<String> ListMap = new HashSet<String>();
	HashMap<String, String> mapValue = new HashMap<String, String>();
	
	public String fn_getLimitZero(){
		for(String keyValue : ListMap){
			Limit_formatLine_Parent += mapValue.get(keyValue) +lineSeperator;
		}
		return Limit_formatLine_Parent +lineSeperator+ Limit_formatLine_Child;
	}
	
	public String fn_limitZero(String configSingleValue) {
		String[] putArray = getIndexOrValue(configSingleValue, null).split(",");
		String limitRef= fn_onlyInsertOriginal(putArray[0]);
		String[] expiryDate = settings.getProperty("EXPIRY.DATE").split(",");
		int expiryDateValue = Integer.parseInt(lineList.get(Integer.parseInt(expiryDate[0].trim())));
		int expiryDateIndex = Integer.parseInt(expiryDate[1].trim());
		
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
			if (expiryDateValue >= expiryDateIndex){
				for (int i = 0 ; i < lineList.size() ; i++) {
					if (i != 0) {
						Limit_formatLine_Child += FIELD_DELIMITER;
					}
					Limit_formatLine_Child += lineList.get(i);
				}
				Limit_formatLine_Child += lineSeperator;
			}
			
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
					if (i == 5 || i == 6 || i == 8 || i == 10 || i == 11){
						int oldDate = Integer.parseInt(oldlineList.get(i));
						int newDate = Integer.parseInt(lineList.get(i));
						if(oldDate < newDate){
							lineList.set(i, Integer.toString(newDate)) ;
						}
						continue;
					}
					
					if (i == 16 || i == 25 || i == 27) {
						BigDecimal oldAmount = getBigDecimal(oldlineList.get(i));
						BigDecimal newAmount = getBigDecimal(lineList.get(i));
						lineList.set(i, (oldAmount.add(newAmount).toString())) ;
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
	public String fn_getEmailFormat() {
		return customer.toString();
	}
	
	public String fn_CustomerFormat(String configSingleValue) {
		//String[] putArray = getIndexOrValue(configSingleValue, null).split(",");
		int emailField = 60;
		String originalString = "_";
		String replaceString = "'_'";
		if (lineList.get(emailField) != "" && lineList.get(emailField).indexOf(originalString.trim()) >= 0) {
			lineList.set(emailField, areplace(lineList.get(emailField), originalString, replaceString, lineList.get(emailField).indexOf(originalString)));
		}
		
		int streetField = 6;
		int addressField = 7; 
		String addressString = lineList.get(addressField);
		int addrStgDlmtrPosition = addressString.indexOf("::"); 
		String newStreet = addrStgDlmtrPosition >= 0 ? addressString.substring(0, addrStgDlmtrPosition) : addressString;
		lineList.set(streetField, newStreet);
		
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
	StringBuilder group = new StringBuilder();
	public String fn_getGroupFormat() {
		return group.toString();
	}
	
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
			if (amountCut > 1) {
				for (int plus = 1 ; plus < amountCut ; plus++) {
					newValue += putArray[0];
					newValue += getContrastValue;
				}
			}
			lineList.set(Integer.parseInt(putArray[pair+1].trim()), newValue);
			
			newValue = "";
			getContrastValue = "";
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
	
	
	
	private String getTOAString (String CompanyCode, String uploadCompanyCode){
		String toaStringKey= uploadCompanyCode+CompanyCode.substring(0,3)+this.lineList.get(Integer.parseInt(settings.getProperty("CY")));
		toaStringKey=settingTOA.getProperty(toaStringKey);
		return toaStringKey;
	}
	
	
	private String getIndexOrValue (String configSingleValue, List<String> lineList){
		String configFieldValue;
		configFieldValue=settings.getProperty(configSingleValue, "").trim();
		
		if(lineList != null){
			configFieldValue = lineList.get(Integer.parseInt(configFieldValue)).trim();
		}
		return configFieldValue;
	}

	private BigDecimal getBigDecimal(String moneyInput){
		BigDecimal moneyOutput = new BigDecimal (moneyInput).setScale(4,BigDecimal.ROUND_DOWN);
		return moneyOutput;
	}
	
	private BigDecimal getBigDecimalNoPoint(String moneyInput){
		BigDecimal moneyOutput = new BigDecimal (moneyInput).setScale(0,BigDecimal.ROUND_DOWN);
		return moneyOutput;
	}
	
	// ¤À¶}
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

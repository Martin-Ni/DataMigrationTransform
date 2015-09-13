package main.java.com.iisi.corebanking.data.migration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class FunctionKeyApp {
	private static char FIELD_DELIMITER;
	private final String lineSeperator;
	private final Properties settings;
	//private final Properties settingsMsg;
	private final Set<String> setFunKey;
	private final ArrayList<Integer> conditionIntList;
	private final ArrayList<String> conditionStringList;
	private final Set<String> setContent;
	private final String migrationDate = "20150911";
	int i= 0;
	String mnemonic = "";
	String msgEQU = "";
	String msgVNV = "";
	String msgV2V = "";
	String msgVSV = "";
	String msgVNN = "";
	String msgSFV = "";
	String msgFVNValue = "";
	String msgFVNBlank = "";
	String msgFVNLD = "";
	String msgFVNLimit = "";
	String msgTCT = "";
	int ix = 0;
	
	public FunctionKeyApp(String outputFileLineSeperator, Properties settings, Properties settingsMsg, char DELIMITER){
		this.settings = settings;
		//this.settingsMsg = settingsMsg;
		this.lineSeperator = outputFileLineSeperator;
		this.FIELD_DELIMITER = DELIMITER;
		this.setFunKey = new HashSet<String>();		
		this.conditionIntList = new ArrayList<Integer>();
		this.conditionStringList = new ArrayList<String>();
		this.setContent = new HashSet<String>();
		addStringToSet(setFunKey, settings .getProperty("fn"));
		
	}
	
	private void addStringToSet(Set<String> set, String commaDelimitedIndices) {
		String[] indices = commaDelimitedIndices.split(",");
		for (String idx : indices) {
			try {
				set.add(idx.trim());
			} catch (NumberFormatException e) {
				// do nothing, the Set remains empty
			}
		}
	}
	
	private void addToList(ArrayList<Integer> set, String commaDelimitedIndices) {
		String[] indices = commaDelimitedIndices.split(",");
		for (String idx : indices) {
			try {
				set.add(Integer.parseInt(idx.trim()));
			} catch (NumberFormatException e) {
				// do nothing, the Set remains empty
			}
		}
	}
	
	private void addToListString(ArrayList<String> set, String commaDelimitedIndices) {
		String[] indices = commaDelimitedIndices.split(",");
		for (String idx : indices) {
			try {
				set.add(idx.trim());
			} catch (NumberFormatException e) {
				// do nothing, the Set remains empty
			}
		}
	}
	
	public void functionKeyAppService(HashMap<Integer,String> oneRecordMap, String mnemonic, int whichLine){

		this.mnemonic= (String) (mnemonic == "NoKey" ? Integer.toString(whichLine) : mnemonic+" - "+Integer.toString(whichLine)); 
		
		if (oneRecordMap != null && oneRecordMap.size()>1) {
			for(String runFnKey :setFunKey){
				switch(runFnKey.substring(0, 3)){
				case "EQS":
					subStringEqSubString(settings.getProperty(runFnKey), oneRecordMap);
					break;
				case "VNV":
					valueNdvalue(settings.getProperty(runFnKey), oneRecordMap);
					break;
				case "V2V":
					valueNdvalueLv2(settings.getProperty(runFnKey), oneRecordMap);
					break;
				case "VSV":
					valuesEqOrLsvalue(settings.getProperty(runFnKey), oneRecordMap);
					break;
				case "VNN":
					valueNdNull(settings.getProperty(runFnKey), oneRecordMap);
					break;
				case "SFV":
					substringFixValue(settings.getProperty(runFnKey), oneRecordMap);
					break;
				case "FVN":
					fixAndValueOrBlank(settings.getProperty(runFnKey), oneRecordMap);
					break;
				case "TCT":
					timeCheckTime(settings.getProperty(runFnKey), oneRecordMap);
					break;
				}
				conditionIntList.clear();
				conditionStringList.clear();
				setContent.clear();
			}
		}
	}
	
	public String getMsg(){
		StringBuilder output = new StringBuilder();
		
		return output.append(
				lineSeperator+
				"====The substring is not match"+lineSeperator+
				msgEQU+lineSeperator+lineSeperator+
				"====When A has value, B also has value"+lineSeperator+
				msgVNV+lineSeperator+lineSeperator+
				msgV2V+lineSeperator+lineSeperator+
				"====When A has value, A also has equals or more then B"+lineSeperator+
				msgVSV+lineSeperator+lineSeperator+
				"====When A has value, B cannot have value"+lineSeperator+
				msgVNN+lineSeperator+lineSeperator+
				"====The substring is not match the fix value"+lineSeperator+
				msgSFV+lineSeperator+lineSeperator+
				"====If A equals fixed value, B need to have the Value"+lineSeperator+
				msgFVNValue+lineSeperator+lineSeperator+
				"====If A equals fixed value, B need to have the Blank"+lineSeperator+
				msgFVNBlank+lineSeperator+lineSeperator+
				"====For LD Product List"+lineSeperator+
				msgFVNLD+lineSeperator+lineSeperator+
				msgFVNLimit+lineSeperator+lineSeperator+
				"====Check the time "+lineSeperator+
				msgTCT+lineSeperator+lineSeperator
				).toString();
		
	}
	
	private void substringFixValue(String condition, HashMap<Integer,String> oneRecordMap) {
		addToList(conditionIntList, condition);
		addStringToSet(setContent, condition);
		for (int i = 1 ; i < conditionIntList.size() ; i+=2){
			addStringToSet(setContent, condition+"."+(i+1)/2);
			if (setContent.contains(oneRecordMap.get(0))){
				msgSFV += "["+oneRecordMap.get(0)+"], ";
			}
		}
	}

	private void valueNdNull(String condition, HashMap<Integer,String> oneRecordMap) {
		addToList(conditionIntList, condition);
		String contrast_A ;
		String contrast_B ;
		for(int i = 0 ; i < conditionIntList.size() ; i += 2){
			contrast_A = oneRecordMap.get(conditionIntList.get(i));
			contrast_B = oneRecordMap.get(conditionIntList.get(i+1));
			if((contrast_A == "" &&  contrast_B == "")||
					(contrast_A != "" &&  contrast_B != "")){
				msgVNN += "["+contrast_A +"and" +contrast_B+"], ";
			}
		}
	}
	
	

	private void valueNdvalue(String condition, HashMap<Integer,String> oneRecordMap) {
		addToList(conditionIntList, condition);
		String contrast_A ;
		String contrast_B ;
		for(int i = 0 ; i < conditionIntList.size() ; i += 2){
			contrast_A = oneRecordMap.get(conditionIntList.get(i));
			contrast_B = oneRecordMap.get(conditionIntList.get(i+1));
			if((contrast_A.split("::").length != contrast_B.split("::").length) ||
					(contrast_A == "" &&  contrast_B != "") ||
					(contrast_A != "" &&  contrast_B == "")){
				msgVNV += "["+contrast_A +" - " +contrast_B+"]-"+this.mnemonic+", "+lineSeperator;
			}
		}
	}
	
	private void valueNdvalueLv2(String condition, HashMap<Integer,String> oneRecordMap) {
		addToList(conditionIntList, condition);
		String contrast_A ;
		String contrast_B ;
		for (int i = 0 ; i < conditionIntList.size() ; i += 2) {
			contrast_A = oneRecordMap.get(conditionIntList.get(i));
			contrast_B = oneRecordMap.get(conditionIntList.get(i+1));
			String[] contrastArry_A =contrast_A.split("::");
			String[] contrastArry_B =contrast_B.split("::");
			
			if((contrastArry_A.length != contrastArry_B.length) ||
					(contrast_A == "" &&  contrast_B != "")||
					(contrast_A != "" &&  contrast_B == "")){
				msgV2V += "["+contrast_A +" - " +contrast_B+"]-"+this.mnemonic+", "+lineSeperator;
			} else {
				int countAmount = contrastArry_A.length;
				for (int ii = 0 ; ii <countAmount ; ii++) {
					int a = contrastArry_A[ii].split("!!").length;
					int b = contrastArry_B[ii].split("!!").length;
					if (a != b) {
						msgV2V += "["+contrast_A +" - " +contrast_B+"]-"+this.mnemonic+", "+lineSeperator;
						break;
					}
				}
			}
		}
	}
	
	private void valuesEqOrLsvalue(String condition, HashMap<Integer,String> oneRecordMap) {
		addToList(conditionIntList, condition);
		String contrast_A ;
		String contrast_B ;
		for(int i = 0 ; i < conditionIntList.size() ; i += 2){
			contrast_A = oneRecordMap.get(conditionIntList.get(i));
			contrast_B = oneRecordMap.get(conditionIntList.get(i+1));
			if ((contrast_A.split("::").length < contrast_B.split("::").length)) {
				msgVSV += "["+contrast_A +" - " +contrast_B+"]-"+this.mnemonic+", "+lineSeperator;
			}
			
		}
	}
	

	private void subStringEqSubString(String condition, HashMap<Integer,String> oneRecordMap){
		addToList(conditionIntList, condition);
		String contrast_A ;
		String contrast_B ;
		for(int i = 0 ; i < conditionIntList.size() ; i += 6){
			contrast_A = oneRecordMap.get(conditionIntList.get(i)).substring(conditionIntList.get(i+1), conditionIntList.get(i+2));
			contrast_B = oneRecordMap.get(conditionIntList.get(i+3)).substring(conditionIntList.get(i+4), conditionIntList.get(i+5));
			if (!contrast_A.equals(contrast_B)){
				msgEQU +="["+oneRecordMap.get(conditionIntList.get(i))+"not match "+oneRecordMap.get(conditionIntList.get(i+3))+"]-"+this.mnemonic+", ";
			}
		}
	}
	
	private void fixAndValueOrBlank(String condition, HashMap<Integer,String> oneRecordMap){
		addToListString(conditionStringList, condition);//FVN= ValueOrBlank, 1, Y, 3
		switch (conditionStringList.get(0)) {
		case "VALUE":
			for (int i = 1 ; i < conditionStringList.size() ; i += 3) {
				String ifValueOfThisFile = oneRecordMap.get(Integer.parseInt(conditionStringList.get(i)));
				String theValueIsThisValue = conditionStringList.get(i+1);
				String thisFileNeedToValue = oneRecordMap.get(Integer.parseInt(conditionStringList.get(i+2))); 
				if (ifValueOfThisFile.equals(theValueIsThisValue) && thisFileNeedToValue.equals("")) {
					msgFVNValue += "["+conditionStringList.get(i)+":"+conditionStringList.get(i+2)+" should be Value ] "+this.mnemonic+", ";
				}
			}
			break;
			
		case "BLANK":
			for (int i = 1 ; i < conditionStringList.size() ; i += 3) {
				String ifValueOfThisFile = oneRecordMap.get(Integer.parseInt(conditionStringList.get(i)));
				String theValueIsThisValue = conditionStringList.get(i+1);
				String thisFileNeedToBlank = oneRecordMap.get(Integer.parseInt(conditionStringList.get(i+2))); 
				if (ifValueOfThisFile.equals(theValueIsThisValue) && !thisFileNeedToBlank.equals("")) {
					msgFVNBlank += "["+conditionStringList.get(i)+":"+conditionStringList.get(i+2)+" should be Blank ] "+this.mnemonic+", ";
				}
			}
			break;
			
		case "LD":
			String recordLimitRef = oneRecordMap.get(8).substring(0, 4).trim();
			String recordCategory = oneRecordMap.get(11).trim();
			String recordSubProduct = oneRecordMap.get(172).trim();
			String recordContract = oneRecordMap.get(170).trim();
			
			String[] propertyValue = settings.getProperty(recordContract.replace(" ", ""), ", ,").split(",", -1);
			String shouldLimitRef = propertyValue[0].trim();
			String shouldCategory = propertyValue[1].trim();
			String shouldSubProduct = propertyValue[2].trim();
			
			if (shouldLimitRef.equals("")) {
				msgFVNLD += "[the Contract is not exist "+recordContract+" ]" +this.mnemonic +", ";
			} 
			else if (!recordLimitRef.equals(shouldLimitRef)) {
				msgFVNLD += "[ Limit.ref are not match "+recordContract+" ] " +this.mnemonic +", ";
			} else if ( !recordCategory.equals(shouldCategory)) {
				msgFVNLD += "[ Category are not match "+recordContract+" ] " +this.mnemonic +", ";
			} else if ( !recordSubProduct.equals(shouldSubProduct)) {
				msgFVNLD += "[ SubProduct are not match "+recordContract+" ] " +this.mnemonic +", ";
			}
			break;
			
		case "LIMIT":
			String recordContract_L = oneRecordMap.get(70).replace(" ", "");
			String recordLimitRef_L = oneRecordMap.get(1).split("\\.")[1].trim();
			String shouldLimitRef_L = settings.getProperty(recordContract_L, ",").split(",", -1)[0].trim();
			if (shouldLimitRef_L.equals("")) {
				msgFVNLimit += "[Contract or Limit.ref is not exist "+oneRecordMap.get(70)+" ] " +this.mnemonic +", ";
			} else if ((shouldLimitRef_L.equals("100") && recordLimitRef_L.equals("0000100")) || 
					(recordLimitRef_L.equals("000"+shouldLimitRef_L))) {
			} else {
				msgFVNLimit += "[ Contract and Limit.ref are not match "+oneRecordMap.get(70)+" ] " +this.mnemonic +", ";
			}
			break;
			
		case "CUSTOMER":
			String recordCustomerId = oneRecordMap.get(1).trim();
			
			String recordSector = oneRecordMap.get(17).trim();
			String recordIndustry = oneRecordMap.get(20).trim();
			
			String shouldSector = settings.getProperty("C"+recordCustomerId, "").split(",")[0].trim();
			String shouldIndustry = settings.getProperty("C"+recordCustomerId, "");
			shouldIndustry = shouldIndustry.equals("") ? shouldIndustry : shouldIndustry.split(",")[1].trim();
			
			if (!shouldSector.equals("")) {
				if (!shouldSector.equals(recordSector) ) {
					msgFVNLimit += "[ CsutomerID '"+recordCustomerId+"' and Sector '"
				+recordSector+"' are not match should be '"
				+shouldSector+"' ] " +this.mnemonic +", ";
				} else if (!shouldIndustry.equals(recordIndustry)) {
					msgFVNLimit += "[ CsutomerID '"+recordCustomerId+"' and Industry '"
				+recordIndustry+"' are not match should be '"
				+shouldIndustry+"' ] " +this.mnemonic +", ";
				}
			} else {
				shouldIndustry = settings.getProperty("S"+recordSector).trim();
				if (!shouldIndustry.equals(recordIndustry)) {
					msgFVNLimit += "[ CsutomerID '"+recordCustomerId+"' and Sector '"
				+recordSector+"' are not match the Industry '"
				+recordIndustry+"', should be '"
				+shouldIndustry+"' ] " +this.mnemonic +", ";
				}
			}
			
			String recordResidence = oneRecordMap.get(24).trim();
			String shouldResidence = settings.getProperty("CR"+recordCustomerId, "").trim();
			
			if (!shouldResidence.equals("")) {
				if (!shouldResidence.equals(recordResidence)) {
					msgFVNLimit += "[ CsutomerID '"+recordCustomerId+"' and Residence '"
				+recordResidence+"' are not match should be '"
				+shouldResidence+"' ] " +this.mnemonic +", ";
				}
			}
			break;
			
		case "PD":
			//System.out.println(i++);
			if (oneRecordMap.get(2).equals("NEW")) {
				String recordContract_P = oneRecordMap.get(26);
				
				String recordLimit_P = oneRecordMap.get(6).trim();
				String recordCategory_P = oneRecordMap.get(5).trim();
				String recordSubproduct_P = oneRecordMap.get(28).trim();
				
				String shouldCategory_P = settings.getProperty(recordContract_P.replace(" ", ""), ", , ").split(",",-1)[1].trim();
				String shouldSubproduct_p = settings.getProperty(recordContract_P.replace(" ", ""), ", , ").split(",",-1)[2].trim();
				
				System.out.println(i++);
				if (recordLimit_P.substring(0, 3).equals("100") && !recordCategory_P.equals(shouldCategory_P)) {
					msgFVNLimit += "[The Category is not match "+recordContract_P+" ] "+this.mnemonic+", ";
					
				} else if (!recordLimit_P.substring(0, 3).equals("100") && !recordCategory_P.equals(shouldCategory_P)) {
					msgFVNLimit += "[The Category is not match "+recordContract_P+" ] "+this.mnemonic+", ";
					
				} else if (!recordLimit_P.substring(0, 3).equals("100") && !recordSubproduct_P.equals(shouldSubproduct_p)) {
					msgFVNLimit += "[The Subproduct is not match "+recordContract_P+" ] "+this.mnemonic+", ";
					
				} else if (shouldCategory_P.equals("")) {
					msgFVNLimit += "[Cannot find the Mapping "+recordContract_P+" ] "+this.mnemonic+", ";
					
				}
			}
			break;
			case "ACCOUNT":
					String recordAccount_A = oneRecordMap.get(1);
					
					String recordCategory_A = oneRecordMap.get(3).trim();
					
					String shouldCategory_A = settings.getProperty("C"+recordAccount_A.trim(), "");
					
					if (!shouldCategory_A.equals("") &&  !shouldCategory_A.equals(recordCategory_A)){
						msgFVNLimit += "[ CsutomerID '"+recordAccount_A+"' and Cateogry '"
								+recordCategory_A+"' are not match should be '"
								+shouldCategory_A+"' ] " +this.mnemonic +", ";
					}
				break;
		}
	}
	
	
	private void timeCheckTime (String condition, HashMap<Integer,String> oneRecordMap) {
		addToListString(conditionStringList, condition); //TCT: 0, 0, 8, <=, 1, 0, 8 ~loop
		String msgStart = "";
		for (int i = 0 ; i < conditionStringList.size() ; i += 7) {
			String Date_A_String = setTime(conditionStringList.get(i), Integer.parseInt(conditionStringList.get(i+1)), Integer.parseInt(conditionStringList.get(i+2)), oneRecordMap);
			String Date_B_String = setTime(conditionStringList.get(i+4), Integer.parseInt(conditionStringList.get(i+5)), Integer.parseInt(conditionStringList.get(i+6)), oneRecordMap);

			if (Date_A_String == "" || Date_B_String == "" || Date_A_String == null ||Date_A_String == null){
				continue;
			}
			int Date_A = Integer.parseInt(Date_A_String);
			int Date_B = Integer.parseInt(Date_B_String);

			switch (conditionStringList.get(i+3)) {
				case "<=":
					if (Date_A > Date_B){
						msgStart += 
								conditionStringList.get(i) +" "+
								conditionStringList.get(i+3)+" "+
								conditionStringList.get(i+4) +" : "+
								Date_A +" should be <= "+Date_B + " | ";
					}
					break;

				case "=<":
					if (Date_A > Date_B){
						msgStart += 
								conditionStringList.get(i) +" "+
								conditionStringList.get(i+3)+" "+
								conditionStringList.get(i+4) +" : "+
								Date_A +" should be <= "+Date_B + " | ";
					}
					break;
					
				case "<":
					if (Date_A >= Date_B){
						msgStart += 
								conditionStringList.get(i) +" "+
								conditionStringList.get(i+3)+" "+
								conditionStringList.get(i+4) +" : "+
								Date_A +" should be < "+Date_B + " | ";
					}
					break;
			
				case "==":
					if (Date_A != Date_B){
						msgStart += 
								conditionStringList.get(i) +" "+
								conditionStringList.get(i+3)+" "+
								conditionStringList.get(i+4) +" : "+
								Date_A +" should be == "+Date_B + " | ";
					}
					break;

				case ">":
					if (Date_A <= Date_B){
						msgStart += 
								conditionStringList.get(i) +" "+
								conditionStringList.get(i+3)+" "+
								conditionStringList.get(i+4) +" : "+
								Date_A +" should be > "+Date_B + " | ";
					}
					break;

				case ">=":
					if (Date_A < Date_B){
						msgStart += 
								conditionStringList.get(i) +" "+
								conditionStringList.get(i+3)+" "+
								conditionStringList.get(i+4) +" : "+
								Date_A +" should be >= "+Date_B + " | ";
					} 
					break;
					
				case "=>":
					if (Date_A < Date_B){
						msgStart += 
								conditionStringList.get(i) +" "+
								conditionStringList.get(i+3)+" "+
								conditionStringList.get(i+4) +" : "+
								Date_A +" should be => "+Date_B + " | ";
					}
					break;
			}
		}
		msgTCT += msgStart != "" ? msgStart +" - "+ this.mnemonic + ", " : "";
	}
	
	
	private String setTime (String condition, int start, int end, HashMap<Integer,String> oneRecordMap){
		if (condition.equals("MD")) {
			condition = migrationDate;
		} else if (condition.length() == 8) {
			
		} else {
			condition = oneRecordMap.get(Integer.parseInt(condition)) != "" ? oneRecordMap.get(Integer.parseInt(condition)).substring(start,end) : oneRecordMap.get(condition);
		}
		
		return condition;
	}
	
	
}

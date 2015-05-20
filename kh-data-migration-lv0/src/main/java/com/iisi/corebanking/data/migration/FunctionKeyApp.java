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
	
	String mnemonic = "";
	String msgEQU = "";
	String msgVNV = "";
	String msgV2V = "";
	String msgVSV = "";
	String msgVNN = "";
	String msgSFV = "";
	String msgFVNValue = "";
	String msgFVNBlank = "";
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

		this.mnemonic= (String) (mnemonic == "NoKey"? Integer.toString(whichLine) : mnemonic+" - "+Integer.toString(whichLine)); 
		
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
				msgFVNBlank+lineSeperator+lineSeperator
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
				msgVNV += "["+contrast_A +" - " +contrast_B+"]-"+this.mnemonic+", ";
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
				msgV2V += "["+contrast_A +" - " +contrast_B+"]-"+this.mnemonic+", ";
			} else {
				int countAmount = contrastArry_A.length;
				for (int ii = 0 ; ii <countAmount ; ii++) {
					int a = contrastArry_A[ii].split("!!").length;
					int b = contrastArry_B[ii].split("!!").length;
					if (a != b) {
						msgV2V += "["+contrast_A +" - " +contrast_B+"]-"+this.mnemonic+", ";
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
				msgVSV += "["+contrast_A +" - " +contrast_B+"]-"+this.mnemonic+", ";
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
		}
	}
	
}

package main.java.com.iisi.corebanking.data.migration;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class DataTransformer {
	// Constants for internal setting
	private static final char FIELD_DELIMITER = '|';
	private static final int FIELD_MAX_LENGTH = 35;
	
	private static final String MAP_REGEX_FIELD_VALUE_KEY = "regex.map.equal";
	private static final String MAP_REGEX_INITIAL_FIELD_VALUE_KEY = "regex.map.initial.equal";
	private static final String EXPECTED_FIELD_COUNT_KEY = "fields.expected.count";
	
	// Default values
	private final Charset charset;
	private final String lineSeperator;
	private final Properties settings;
	private int fieldCount;
	//private final Properties settingsMsg;
	private final Set<Integer> mapFieldSet;
	private final Set<Integer> mapInitialFieldSet;
	private final Set<String> setChar;
	private final Set<String> setLine;
	private final Set<String> setField;
	private final Set<String> setFixValue;
	private final Set<String> setAmountField;
	private HashMap<String, String> mappingLength;
	private HashMap<String, String> mappingField;
	private HashMap<String, String> mappingFieldChar;
	private HashMap<String, String> mappingDataLine;
	private HashMap<String, String> mappingDataFieldLength;
	private HashMap<String, String> mappingDataFieldLeast;
	private HashMap<String, String> mappingDataFieldNotMatchFixValue;
	private HashMap<String, String> mappingDataLineAndField;
	private String mnemonic ="NoKey";
	private FunctionKeyApp fnKey;
	/**
	 * 依照所給定的設定建立一個DataTransformer實體。輸出檔案將會使用系統預設的換行字元。
	 * 
	 * @param charsetName
	 *            - 輸入檔案的編碼名稱
	 * @param settings
	 *            - 含有設定資訊的Properties
	 * @throws IllegalCharsetNameException
	 *             If the given charset name is illegal
	 * @throws IllegalArgumentException
	 *             If the given charsetName is null
	 * @throws UnsupportedCharsetException
	 *             If no support for the named charset is available in this
	 *             instance of the Java virtual machine
	 */
	public DataTransformer(String charsetName, Properties settings, Properties settingsMsg) {
		this(Charset.forName(charsetName), settings, settingsMsg);
	}

	/**
	 * 依照所給定的設定建立一個DataTransformer實體。輸出檔案將會使用系統預設的換行字元。
	 * 
	 * @param charset
	 *            - 輸入檔案的編碼
	 * @param settings
	 *            - 含有設定資訊的Properties
	 */
	public DataTransformer(Charset charset, Properties settings, Properties settingsMsg) {
		this(charset, System.getProperty("line.separator"), settings, settingsMsg);
	}

	/**
	 * 依照所給定的設定建立一個DataTransformer實體
	 * 
	 * @param charset
	 *            - 輸入檔案的編碼
	 * @param outputFileLineSeperator
	 *            - 輸出檔案所要使用的換行字元
	 * @param settings
	 *            - 含有設定資訊的Properties
	 */
	public DataTransformer(Charset charset, String outputFileLineSeperator, Properties settings, Properties settingsMsg) {
		super();
		this.charset = charset;
		this.lineSeperator = outputFileLineSeperator;
		this.settings = settings;
		//this.settingsMsg = settingsMsg;
		this.mapFieldSet = new HashSet<Integer>();
		this.mapInitialFieldSet = new HashSet<Integer>();
		this.setChar = new HashSet<String>();
		this.setLine = new HashSet<String>();
		this.setField = new HashSet<String>();
		this.setFixValue = new HashSet<String>();
		this.setAmountField = new HashSet<String>();
		this.mappingLength = new HashMap<String, String>();
		this.mappingField = new HashMap<String, String>();
		this.mappingFieldChar =new HashMap<String, String>();
		this.mappingDataLine = new HashMap<String, String>();
		this.mappingDataFieldLength = new HashMap<String, String>();
		this.mappingDataFieldLeast = new HashMap<String, String>();
		this.mappingDataFieldNotMatchFixValue = new HashMap<String, String>();
		this.mappingDataLineAndField = new HashMap<String, String>();
		
		if(!this.settings.getProperty("fn","").equals("")){
			this.fnKey = new FunctionKeyApp(outputFileLineSeperator, settings, settingsMsg, FIELD_DELIMITER);
		}
		init();
	}

	/**
	 * 初始化實體變數
	 */
	private void init() {
		String mapFieldIdx = settings.getProperty(MAP_REGEX_FIELD_VALUE_KEY);
		String mapInitialFieldIdx = settings.getProperty(MAP_REGEX_INITIAL_FIELD_VALUE_KEY);
		if (!isEmptyString(mapFieldIdx)) {
			addIntToSet(mapFieldSet, mapFieldIdx);
		}
		if (!isEmptyString(mapInitialFieldIdx)) {
			addIntToSet(mapInitialFieldSet, mapInitialFieldIdx);
		}
		
		// 讀取所設定的欄位數量
		String fieldCountStr = settings.getProperty(EXPECTED_FIELD_COUNT_KEY);
		if (fieldCountStr != null && fieldCountStr.trim().length() != 0) {
			try {
				fieldCount = Integer.parseInt(fieldCountStr);
			} catch (NumberFormatException e) {
				
			}
		}
		
	}

	/**
	 * 將以逗號分隔的多個欄位索引分割並trim後轉為Integer加入Set。 若所分割出來的字串無法被轉換為Integer，則會略過該筆資料。
	 * 
	 * @param set
	 *            - 要加入資料的Set
	 * @param commaDelimitedIndices
	 *            - 逗號分割的欄位索引
	 */
	private void addIntToSet(Set<Integer> set, String commaDelimitedIndices) {
		String[] indices = commaDelimitedIndices.split(",");
		set.clear();
		for (String idx : indices) {
			try {
				set.add(Integer.parseInt(idx.trim()));
			} catch (NumberFormatException e) {
				// do nothing, the Set remains empty
			}
		}
	}
	
	private void addStringToSet(Set<String> set, String commaDelimitedIndices) {
		String[] indices = commaDelimitedIndices.split(",,");
		set.clear();
		for (String idx : indices) {
			try {
				set.add(idx.trim());
			} catch (NumberFormatException e) {
				// do nothing, the Set remains empty
			}
		}
	}

	/**
	 * 判斷字串是否無意義
	 * 
	 * @param string
	 *            - 要判斷的字串
	 * @return 判斷結果，即 string != null || string.trim().length() == 0
	 */
	private boolean isEmptyString(String string) {
		return string == null || string.trim().length() == 0;
	}

	/**
	 * 轉換檔案內容
	 * 
	 * @param inputFile
	 *            - 資料輸入來源檔案
	 * @param outputFile
	 *            - 資料輸出目的檔案
	 * @throws IOException
	 *             如果IO發生錯誤
	 * @throws IllegalArgumentException
	 *             所給定的任一參數之指向為資料夾而非檔案
	 */
	public void transform(File inputFile, File outputFile) throws IOException, IllegalArgumentException {
		if (inputFile.isDirectory()) {
			throw new IllegalArgumentException(inputFile + " is a Directory");
		}
		if (outputFile.isDirectory()) {
			throw new IllegalArgumentException(outputFile + " is a Directory");
		}

		FileInputStream fr = new FileInputStream(inputFile);
		InputStreamReader in = new InputStreamReader(fr, charset);
		@SuppressWarnings("resource")
		BufferedReader br = new BufferedReader(in);
		String line = "";

		StringBuilder output = new StringBuilder();
		HashMap<Integer,String> oneRecordMap = new HashMap <Integer,String>();
		int whichLine = 0;
		int whichField = 0;
		int whichChar = 0;
		String fieldValue = "";
		while (true) {
			line = br.readLine();
			whichLine++;
			if (line == null) {
				break;
			}
			
			mnemonic = settings.getProperty("KEY","NoKey");
			if(mnemonic != "NoKey"){
				mnemonic = line.split("\\|")[Integer.parseInt(mnemonic)];
			}
			
			char[] lineChar = line.toCharArray();
			whichField=0;
			
			for (int i = 0; i < lineChar.length; i++) {
				if (lineChar[i] == FIELD_DELIMITER) {
					mappingDataFieldLength.put(Integer.toString(whichField), Integer.toString(whichChar));
					identifyFixValue(fieldValue, whichField);
					oneRecordMap.put(whichField, fieldValue);
					whichField++;
					whichChar = 0;
					fieldValue = "";
					continue;
				}
				fieldValue += lineChar[i];
				whichChar++;
				if (lineChar[i] < 0x20 || lineChar[i] >= 0x7F ) {
					byte charByte = (byte) lineChar[i];
					String key =byte2hex(charByte)+" - "+lineChar[i]+" (Not UTF 8)";
					mappingValueAndKey(key, whichLine, whichField);
				} else if (mapFieldSet.contains(whichField)) {
					String conditRegx = settings.getProperty(Integer.toString(whichField)+"E");
					if (!Character.toString(lineChar[i]).matches(conditRegx)){
						byte charByte = (byte) lineChar[i];
						String key =byte2hex(charByte)+" - "+lineChar[i]+" (Not allow)";
						mappingValueAndKey(key, whichLine, whichField);
					}
				}
				
				if (mapInitialFieldSet.contains(whichField) && whichChar == 1) {
					String conditRegx = settings.getProperty(Integer.toString(whichField)+"I");
					if (Character.toString(lineChar[i]).matches(conditRegx)) {
						byte charByte = (byte) lineChar[i];
						String key =byte2hex(charByte)+" - "+lineChar[i]+" (initial) ";
						mappingValueAndKey(key, whichLine, whichField);
					}
				}
			}
			mappingDataFieldLength.put(Integer.toString(whichField), Integer.toString(whichChar));
			identifyAndInitialLength(whichField, whichLine);
			identifyFixValue(fieldValue, whichField);
			if (whichLine%1000  == 0)
			System.out.println(whichLine);
			
			if (fieldCount != whichField+1){
				setAmountField.add(Integer.toString(whichLine) +"-"+ Integer.toString(whichField));
			}
			
			oneRecordMap.put(whichField, fieldValue);
			if(!this.settings.getProperty("fn","").equals("")){
				this.fnKey.functionKeyAppService(oneRecordMap, mnemonic, whichLine);
			}
			oneRecordMap.clear();
			fieldValue = "";
			whichChar = 0;
		}
		System.out.println(whichLine);
		
		
		output.append(
				"Part0===Which field over the length ==== "+lineSeperator+
				sortMap(whichField, mappingLength)+lineSeperator+lineSeperator+
				
				"Part1===Which field Need to provide the value ==== "+lineSeperator+
				sortMap(whichField, mappingDataFieldLeast)+lineSeperator+lineSeperator+
				
				"Part2===Which Record is not match the fields amount ==== "+lineSeperator+
				setAmountField+lineSeperator+lineSeperator+
				
				"Part3===The fix value validation ==== "+lineSeperator+
				sortMap(whichField, mappingDataFieldNotMatchFixValue)+lineSeperator+lineSeperator+
				
				"Part4===The character isn't allowed in which Field==== "+lineSeperator+
				"=====ASCII(HEX) - Character (Description) = Which Field (The Index Start From 0)"+lineSeperator+
				mappingField+lineSeperator+lineSeperator+
				
				"Part5===The Field has which character isn't allowed==== "+lineSeperator+
				"=====Which Field (The Index Start From 0) = <ASCII(HEX) - Character (Description)> "+lineSeperator+
				sortMap(whichField, mappingFieldChar)+lineSeperator+lineSeperator+
				
				"Part6===The character isn't allowed in which Data Line==== "+lineSeperator+
				"=====ASCII(HEX) - Character (Description) = Which Line (The Index Start From 1)"+lineSeperator+	
				mappingDataLine+lineSeperator+lineSeperator+
				/*
				"Part7===The Data Line has the character isn't allowed==== "+lineSeperator+
				"=====Which Line (The Index Start From 1) = <ASCII(HEX) - Character (Description) in which Field (The Index Start From 0)> "+lineSeperator+
				sortMap(whichLine, mappingDataLineAndField)+lineSeperator+lineSeperator+
				*/
				
				"Part8===for function key==== "+
				(String)(!this.settings.getProperty("fn","").equals("") ? fnKey.getMsg()+lineSeperator : "")
				);

		// Write to output file
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile))) {
			bw.write(output.toString());
		}	
		this.setChar.clear();
		this.setLine.clear();
		this.setField.clear();
		this.mappingLength.clear();
		this.mappingField.clear();
		this.mappingFieldChar.clear();
		this.mappingDataLine.clear();
		this.mappingDataFieldLength.clear();
		this.mappingDataFieldLeast.clear();		
		this.mappingDataLineAndField.clear();
		System.gc();
	}
	
	private static String checkExit(String string, String valueOf) {
		String[] indices = string.split("\\|");
		for(int i = 0 ; i<indices.length ; i++){
			if (indices[i].trim().equals(valueOf.trim())){
				return string;
			}
		}
		return string+"| "+valueOf;
	}


	private static String byte2hex(byte b) { // 二進制轉16進制字符
		String hs = "";
		String stmp = (java.lang.Integer.toHexString(b & 0XFF));
			if (stmp.length() == 1) {
				hs = hs + "0" + stmp;
			} else {
				hs = hs + stmp;
			}
		return hs.toUpperCase();
	}
	
	private String sortMap(int amount, HashMap<String, String> mapping){
		String dataString="";
		for(int i = 0 ; i <= amount ; i++){
			String iString =Integer.toString(i);
			if(mapping.containsKey(iString))
				dataString += " "+iString+"="+mapping.get(iString)+lineSeperator;
		}
		return dataString;
	}
	
	
	private void identifyAndInitialLength(int fieldAmount, int whichLine){
		int fieldDataLength = 0;
		int length;
		String cutLengthString;
		//System.out.println(mappingDataLineLength);
		for (int whichField=0 ; whichField <= fieldAmount ; whichField++) {
			fieldDataLength = Integer.parseInt(mappingDataFieldLength.get(Integer.toString(whichField)).trim());
			
			length = FIELD_MAX_LENGTH;
			
			cutLengthString = settings.getProperty(Integer.toString(whichField)+"L", "").trim();
			if (!isEmptyString(cutLengthString)) {
				length = Integer.parseInt(cutLengthString);
			}
			if (fieldDataLength > length) {
				mappingLength.put(Integer.toString(whichField), /*mappingLength.get(Integer.toString(whichField))+ */" Over the Length "+length+" "+whichLine +" ["+ mnemonic+"], ");
			}
			
			cutLengthString = settings.getProperty(Integer.toString(whichField)+"S", "").trim();
			if (!isEmptyString(cutLengthString)){
				length = Integer.parseInt(cutLengthString);
				if (fieldDataLength < length){
				mappingDataFieldLeast.put(Integer.toString(whichField),/*mappingLength.get(Integer.toString(whichField))+ */" Least the Length "+length+" "+whichLine+" ["+ mnemonic+"], ");
				}
			}
		}
		//System.out.println(mappingLength);
	}
	
	//@SuppressWarnings("null")
	private void identifyFixValue(String fieldValue, int whichField){
		String fixValue =settings.getProperty(whichField+"F", "").trim();
		String needValue =settings.getProperty(whichField+"N", "").trim();
		if (!isEmptyString(fieldValue) && (!isEmptyString(fixValue) || !isEmptyString(needValue))) {
			String whichFieldString = Integer.toString(whichField);
			String separateDelimiter = settings.getProperty(whichField+"R", "").trim();
			String[] separateValue;
			if (separateDelimiter == "") {
				separateValue = new String[1];
				separateValue[0] =fieldValue; 
			} else {
				separateValue = fieldValue.split(separateDelimiter); 
			}
			
			
			if (!isEmptyString(fixValue)) {
				for (String fieldValueString : separateValue) {
					addStringToSet(setFixValue, fixValue);
					if (!setFixValue.contains(fieldValueString)) {
						if (mappingDataFieldNotMatchFixValue.containsKey(whichFieldString)){
							mappingDataFieldNotMatchFixValue.put(whichFieldString, 
									checkExit(mappingDataFieldNotMatchFixValue.get(whichFieldString), "<Not allow this String: "+fieldValue+" - "+mnemonic+">")
									);
						} else {
							mappingDataFieldNotMatchFixValue.put(whichFieldString, " <Not allow this String: "+fieldValue+" - "+mnemonic+">");
						}
						break;
					}
				}
			}
			
			
			if (!isEmptyString(needValue)) {
				for (String fieldValueString : separateValue) {
					if (!fieldValueString.trim().matches(needValue)) {
							if (mappingDataFieldNotMatchFixValue.containsKey(whichFieldString)) {
								mappingDataFieldNotMatchFixValue.put(whichFieldString,
										checkExit(mappingDataFieldNotMatchFixValue.get(whichFieldString), /*mnemonic)*/"<The Invalid Format:  ["+mnemonic+"] "+fieldValue+">")
										);
							} else {
								mappingDataFieldNotMatchFixValue.put(whichFieldString, /*mnemonic);*/" <The Invalid Format:  ["+mnemonic+"] "+fieldValue+">");
							}
						break;
					}
				}
			}
		}
	}
	
	
	private void mappingValueAndKey(String key, int whichLine, int whichField) { // 二進制轉16進制字符
		String whichLineString = Integer.toString(whichLine);
		String whichFieldString = Integer.toString(whichField);
		
		if (this.setField.contains(whichFieldString)) {
			this.mappingFieldChar.put(whichFieldString, /*this.mappingFieldChar.get(whichFieldString)+ " <"+key+">");*/checkExit(this.mappingFieldChar.get(whichFieldString), " <"+key+"> "));
		} else {
			this.setField.add(whichFieldString);
			this.mappingFieldChar.put(whichFieldString, " <"+key+">");
		}
		
		if (this.setChar.contains(key)) {
			this.mappingField.put(key, checkExit(this.mappingField.get(key), whichFieldString));
			this.mappingDataLine.put(key, this.mappingDataLine.get(key)+"| "+whichLineString);
		} else {
			this.setChar.add(key);
			this.mappingField.put(key, " "+whichFieldString);
			this.mappingDataLine.put(key, " "+whichLineString);
		}
		
		if (this.setLine.contains(whichLineString)) {
			this.mappingDataLineAndField.put(whichLineString, this.mappingDataLineAndField.get(whichLineString)+"| "+"<"+key+" in "+whichFieldString+">");
		} else {
			this.setLine.add(whichLineString);
			this.mappingDataLineAndField.put(whichLineString, " <"+key+" in "+whichFieldString+">");
		}
	}
}

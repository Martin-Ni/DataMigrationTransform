package com.iisi.corebanking.data.migration;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class DataTransformer {
	// Constants for internal setting
	private static final char FIELD_DELIMITER = '|';
	private static final int FIELD_MAX_LENGTH = 35;
	private static final int NO_CONDITION_FILED_INDICATOR = 0;
	private static final int MULTI_FILED_INDICATOR = 1;
	private static final int SUB_FIELD_INDICATOR = 2;
	// Property keys
	private static final String NO_CONDITION_VALUE_FIELDS_KEY = "fields.no.condition";
	private static final String MULTI_VALUE_FIELDS_KEY = "fields.multi";
	private static final String SUB_VALUE_FIELDS_KEY = "fields.sub";
	
	private static final String MULTI_VALUE_FIELDS_REPLACE_KEY = "fields.replace.multi";
	private static final String SUB_VALUE_FIELDS_REPLACE_KEY = "fields.replace.sub";
	
	private static final String NO_CONDITION_VALUE_DELIMITER_KEY = "fields.no.condition.delimiter";
	private static final String MULTI_VALUE_DELIMITER_KEY = "fields.multi.delimiter";
	private static final String SUB_VALUE_DELIMITER_KEY = "fields.sub.delimiter";
	
	private static final String EXPECTED_FIELD_COUNT_KEY = "fields.expected.count";
	
	private static final String LENGTH_EXCEEDED_MSG_KEY = "fields.single.error";
	private static final String SPECIFIED_FIELD_COUNT_MSG_KEY = "fields.specifid.error";
	private static final String NOT_MATCH_FIELD_COUNT_MSG_KEY = "fields.not.match.error";
	
	// Default values
	private static String LENGTH_EXCEEDED_DEFAULT_MSG = "";//{[CAUTION] field data length exceed " + FIELD_MAX_LENGTH + "}
	private static String SPECIFIED_FIELD_COUNT_DEFAULT_MSG ="";
	private static String NOT_MATCH_FIELD_COUNT_DEFAULT_MSG ="";
	
	private static final String NO_CONDITION_DEFAULT_DELIMITER = "";
	private static final String MULTI_VALUE_DEFAULT_DELIMITER = "::";
	private static final String SUB_VALUE_DEFAULT_DELIMITER = "!!";
	
	private final Charset charset;
	private final String lineSeperator;
	private final Properties settings;
	private final Properties settingsMsg;
	private final Set<Integer> noConValueIdxSet;
	private final Set<Integer> multiValueIdxSet;
	private final Set<Integer> subValueIdxSet;
	private final Set<Integer> multiValueRepIdxSet;
	private final Set<Integer> subValueRepIdxSet;
	private int fieldCount;
	private String recordPrefix;

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
		this.settingsMsg = settingsMsg;
		this.noConValueIdxSet = new HashSet<Integer>();
		this.multiValueIdxSet = new HashSet<Integer>();
		this.subValueIdxSet = new HashSet<Integer>();
		this.multiValueRepIdxSet = new HashSet<Integer>();
		this.subValueRepIdxSet = new HashSet<Integer>();
		this.fieldCount = -1;
		this.recordPrefix = "";
		init();
	}

	/**
	 * 初始化實體變數
	 */
	private void init() {
		LENGTH_EXCEEDED_DEFAULT_MSG = this.settingsMsg.getProperty(LENGTH_EXCEEDED_MSG_KEY, LENGTH_EXCEEDED_DEFAULT_MSG);
		SPECIFIED_FIELD_COUNT_DEFAULT_MSG = this.settingsMsg.getProperty(SPECIFIED_FIELD_COUNT_MSG_KEY, SPECIFIED_FIELD_COUNT_DEFAULT_MSG);
		NOT_MATCH_FIELD_COUNT_DEFAULT_MSG = this.settingsMsg.getProperty(NOT_MATCH_FIELD_COUNT_MSG_KEY, NOT_MATCH_FIELD_COUNT_DEFAULT_MSG);
		
		String noConIdx = settings.getProperty(NO_CONDITION_VALUE_FIELDS_KEY);
		String multiIdx = settings.getProperty(MULTI_VALUE_FIELDS_KEY);
		String subIdx = settings.getProperty(SUB_VALUE_FIELDS_KEY);
		String multiRepIdx = settings.getProperty(MULTI_VALUE_FIELDS_REPLACE_KEY);
		String subRepIdx = settings.getProperty(SUB_VALUE_FIELDS_REPLACE_KEY);

		if (!isEmptyString(noConIdx)) {
			addToSet(noConValueIdxSet, noConIdx);
		}
		
		if (!isEmptyString(multiIdx)) {
			addToSet(multiValueIdxSet, multiIdx);
		}

		if (!isEmptyString(subIdx)) {
			addToSet(subValueIdxSet, subIdx);
		}
		
		if (!isEmptyString(multiRepIdx)) {
			addToSet(multiValueRepIdxSet, multiRepIdx);
		}
		
		if (!isEmptyString(subRepIdx)) {
			addToSet(subValueRepIdxSet, subRepIdx);
		}
		
		// 讀取所設定的欄位數量
		String fieldCountStr = settings.getProperty(EXPECTED_FIELD_COUNT_KEY);
		if (fieldCountStr != null && fieldCountStr.trim().length() != 0) {
			try {
				fieldCount = Integer.parseInt(fieldCountStr);
			} catch (NumberFormatException e) {
				//recordPrefix = "( [WARNING] field count specified but can not be parse: \"" + fieldCountStr + "\" )";
				recordPrefix = SPECIFIED_FIELD_COUNT_DEFAULT_MSG;
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
	private void addToSet(Set<Integer> set, String commaDelimitedIndices) {
		String[] indices = commaDelimitedIndices.split(",");
		for (String idx : indices) {
			try {
				set.add(Integer.parseInt(idx.trim()));
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

		// Read Data from File
		Path filePath = Paths.get(inputFile.toURI());
		List<String> allLines = Files.readAllLines(filePath, charset);
		int whichLine=0;
		// Process Each line
		StringBuilder output = new StringBuilder();
		boolean notFirst = false;
		for (String line : allLines) {
			// Skip empty line
			whichLine++;
			System.out.println(whichLine);
			if (line == null || line.trim().length() == 0) {
				continue;
			}
			String processedLine = processRecord(line);
			if (notFirst) {
				output.append(lineSeperator);
			} else {
				notFirst = true;
			}
			
			output.append(processedLine);
		}

		// Write to output file
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile))) {
			bw.write(output.toString());
		}
	}

	/**
	 * 處理資料(一行)
	 * 
	 * @param line
	 *            - 要處理的資料
	 * @return 處理過的資料
	 */
	private String processRecord(String line) {

		if (line == null || line.indexOf(FIELD_DELIMITER) < 0) {
			return line;
		}
		// **** Step 1 -- Split record into fields ****
		List<String> list = new ArrayList<String>();
		// add substring before the first delimiter
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

		// **** Step 2 -- Process each record ****
		StringBuilder processedRecord = new StringBuilder(recordPrefix);

		// check field count number
		if (fieldCount != -1 && list.size() != fieldCount) {
			//Length Message
			//processedRecord.append("( [WARNING] field count mismatch, expected " + fieldCount + " but got " + list.size() + " )");
			processedRecord.append(NOT_MATCH_FIELD_COUNT_DEFAULT_MSG);
		}
		
		String cutLengthString;
		
		
		
		// iterate fields
		for (int i = 0; i < list.size(); i++) {
			// append delimiter if it is not first field
			if (i != 0) {
				processedRecord.append(FIELD_DELIMITER);
			}
			// acquire field data
			String currentFieldData = list.get(i);
			// skip following logic if the data is empty
			if (currentFieldData.length() == 0) {
				processedRecord.append(currentFieldData);
				continue;
			}
			// check field type and delegate to suitable handling method
			
			cutLengthString = settings.getProperty(Integer.toString(i)+"L", "").trim();
			int cutLengthInt = FIELD_MAX_LENGTH;
			if (!isEmptyString(cutLengthString)) {
				cutLengthInt = Integer.parseInt(cutLengthString);
			}
			
			if (noConValueIdxSet.contains(i)) {
				processedRecord.append(handleMultiAndSubField(currentFieldData, NO_CONDITION_FILED_INDICATOR, cutLengthInt ));
			}else if (multiValueIdxSet.contains(i)) {
				processedRecord.append(handleMultiAndSubField(currentFieldData, MULTI_FILED_INDICATOR, cutLengthInt));
			} else if (subValueIdxSet.contains(i)) {
				processedRecord.append(handleMultiAndSubField(currentFieldData, SUB_FIELD_INDICATOR, cutLengthInt));
			} else if (multiValueRepIdxSet.contains(i)) {
				processedRecord.append(handleMultiAndSubFieldReplace(currentFieldData, MULTI_FILED_INDICATOR, cutLengthInt, Integer.toString(i)));
			} else if (subValueRepIdxSet.contains(i)) {
				processedRecord.append(handleMultiAndSubFieldReplace(currentFieldData, SUB_FIELD_INDICATOR, cutLengthInt, Integer.toString(i)));
			} else if (settings.getProperty(i+"A", "") != "") {
				processedRecord.append(repalceAllCondition(currentFieldData, settings.getProperty(i+"A", "")));
			} else {
				processedRecord.append(handleSingleFiled(currentFieldData, cutLengthInt, Integer.toString(i)));
			}
		}
		return processedRecord.toString();
	}

	/**
	 * 處理多值欄位資料(Multi-value或Sub-group)
	 * 
	 * @param currentField
	 *            - 要處理的欄位資料
	 * @param fieldTypeIndicator
	 *            - 指定是處理Multi-value或Sub-group，請使用常數來指定
	 * @return 處理過後的欄位資料
	 * @throws IllegalArgumentException
	 *             如果fieldTypeIndicator所給的值無法解析
	 */
	private String handleMultiAndSubField(String currentField, int fieldTypeIndicator, int cutLengthInt) {
		// skip following logic if length dose not exceed max length

		if (currentField.length() <= cutLengthInt) {
			return currentField;
		}

		// Determine delimiter
		String delimiter;
		switch (fieldTypeIndicator) {
		case NO_CONDITION_FILED_INDICATOR:
			delimiter = settings.getProperty(NO_CONDITION_VALUE_DELIMITER_KEY, NO_CONDITION_DEFAULT_DELIMITER);
			break;
		case MULTI_FILED_INDICATOR:
			delimiter = settings.getProperty(MULTI_VALUE_DELIMITER_KEY, MULTI_VALUE_DEFAULT_DELIMITER);
			break;
		case SUB_FIELD_INDICATOR:
			delimiter = settings.getProperty(SUB_VALUE_DELIMITER_KEY, SUB_VALUE_DEFAULT_DELIMITER);
			break;
		default:
			throw new IllegalArgumentException("Invalid fieldTypeIndicator: " + fieldTypeIndicator);
		}

		// Reconstruct delimited field data
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
	
	
	
	private String handleMultiAndSubFieldReplace(String currentField, int fieldTypeIndicator, int cutLengthInt, String whichField){
		// Determine delimiter
		StringBuilder processedFieldData = new StringBuilder();
		String replaceString = settings.getProperty(whichField+"R").trim();
		if (currentField.indexOf(replaceString) < 0) {
			if(currentField.length() > cutLengthInt){
				processedFieldData.append(LENGTH_EXCEEDED_DEFAULT_MSG + "-"+whichField + " ] ");
			}
			processedFieldData.append(currentField);
		} else {
			String delimiter;
			switch (fieldTypeIndicator) {
			case MULTI_FILED_INDICATOR:
				delimiter = settings.getProperty(MULTI_VALUE_DELIMITER_KEY, MULTI_VALUE_DEFAULT_DELIMITER);
				break;
			case SUB_FIELD_INDICATOR:
				delimiter = settings.getProperty(SUB_VALUE_DELIMITER_KEY, SUB_VALUE_DEFAULT_DELIMITER);
				break;
			default:
				throw new IllegalArgumentException("Invalid fieldTypeIndicator: " + fieldTypeIndicator);
			}
			
			String[] splitStringList = currentField.split(replaceString);
			for(int i = 0 ; i < splitStringList.length ; i++){
				if (splitStringList[i].trim().length() > cutLengthInt){
					processedFieldData.append(LENGTH_EXCEEDED_DEFAULT_MSG+ "-"+whichField + " ] ");
				}
				processedFieldData.append(splitStringList[i].trim());
				if (i != splitStringList.length-1) {
					processedFieldData.append(delimiter);
				}
			}
		}
		
		return processedFieldData.toString();//+LENGTH_EXCEEDED_DEFAULT_MSG;
	}
	
	private String repalceAllCondition(String currentField, String replaceAll){
		StringBuilder processedFieldData = new StringBuilder();
		String[] replaceAllSplit = replaceAll.split(",,", -1);
		String condtionString = "";
		for (int i = 0 ; i < replaceAllSplit.length ; i += 2) {
			if (currentField.equals(replaceAllSplit[i].trim())) {
				condtionString = replaceAllSplit[i+1].trim();
				break;
			} else {
				condtionString = currentField;
			}
		}
		return processedFieldData.append(condtionString).toString();
	}

	/**
	 * 處理單值欄位資料，如果資料太長會加上錯誤訊息
	 * 
	 * @param currentField
	 *            - 要處理的欄位資料
	 * @return 處理過後的欄位資料
	 */
	//Length Message
	private String handleSingleFiled(String currentField, int cutLengthInt, String whichField) {
		if (currentField.length() <= cutLengthInt) {
			return currentField;
		}
		return currentField + LENGTH_EXCEEDED_DEFAULT_MSG+ "-"+whichField + " ] ";
	}

}

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

import main.java.com.iisi.corebanking.data.migration.IllegalCharsetNameException;
import main.java.com.iisi.corebanking.data.migration.UnsupportedCharsetException;

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
	 * Create an entity of Data Transformer. When you Output, it will use the default the encoding of newline.  
	 * 
	 * @param charsetName
	 *            - Input the Name of Encoding with file.
	 * @param settings
	 *            - Input the Properties of Setting information
	 * @param settingsMsg
	 *            - Input the Properties of Setting Error Message
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
	 * Create an entity of Data Transformer. When you Output, it will use the default the encoding of newline.  
	 *  
	 * @param charset
	 *            - Input the encoding of file.
	 * @param settings
	 *            - Input the Properties of Setting information
	 * @param settingsMsg
	 *            - Input the Properties of Setting Error Message
	 */
	public DataTransformer(Charset charset, Properties settings, Properties settingsMsg) {
		this(charset, System.getProperty("line.separator"), settings, settingsMsg);
	}

	/**
	 * Create an entity of Data Transformer base on the Setting Information.
	 * @param charset
	 *            - Input the encoding of file.
	 * @param outputFileLineSeperator
	 *            - Output the character of newline that is used
	 * @param settings
	 *            - Input the Properties of Setting information
	 * @param settingsMsg
	 *            - Input the Properties of Setting Error Message
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
	 * Initialize the variable
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
		
		// Loading the quantity of fields by setting
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
	 * Use the comma to slit the multi reference of Field number and convert to Integer Int and join to Set.
	 * If the string cannot convert to Integer, will ignore this reference one. 
	 * 
	 * @param set
	 *            - Join the data in Set.
	 * @param commaDelimitedIndices
	 *            - Use the comma split the reference field
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
	 * Judge this String whether it's meaningful
	 * 
	 * @param string
	 *            - The String that be juded.
	 * @return Judge the result, if string != null || string.trim().length() == 0
	 */
	private boolean isEmptyString(String string) {
		return string == null || string.trim().length() == 0;
	}

	/**
	 * Start insert the Delimiter Value and checking the Length.
	 * 
	 * @param inputFile
	 *            - Input the Original data file.
	 * @param outputFile
	 *            - Output the path of data file.
	 * @throws IOException
	 *            - If IO Error occur.
	 * @throws IllegalArgumentException
	 *            - Specify the path of folder rather than a file.
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
	 * Dispose the data (one Record)
	 * 
	 * @param line
	 *            - The data for split.
	 * @return processedRecord
	 *            - The record is formated.
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
			if (settings.getProperty(i+"G", "") != "") {
				processedRecord.append(settings.getProperty(i+"G", ""));
				continue;
			} else if (currentFieldData.length() == 0) {
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
	 * Dispose the multi value of the specify fields (Multi-value or Sub-group)
	 * 
	 * @param currentField
	 *            - The value of specify the value
	 * @param fieldTypeIndicator
	 *            - specify the delimiter by Multi-value or Sub-value, Please use the constant to assign.
	 * @return processedFieldData
	 *            - The value is formated after.
	 * @throws IllegalArgumentException
	 *            - If the value of fieldTypeIndicator cannot analyze.
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
				StringBuilder empty = new StringBuilder();
				processedFieldData = empty;
			} else {
				processedFieldData.append(cutData);
			}
		}
		return processedFieldData.toString();
	}
	
	
	/**
	 * Replace the Delimiter to Multi Value or Sub Value.
	 * 
	 * @param currentField
	 *            - Dispose the value need to be formated.
	 * @param fieldTypeIndicator
	 *            - The condition for specify the delimiter to replace.
	 * @param cutLengthInt
	 *            - .Criteria the Value in the Length.
	 * @param whichField
	 *            - For reference the which Field.
	 * @return processedFieldData
	 *            - The value is formated after.
	 */
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
	
	
	/**
	 * Replace the all value to Fixed Value..
	 * 
	 * @param currentField
	 *            - Dispose the value need to be formated.
	 * @param replaceAll
	 *            - New Value to replace.
	 * @return processedFieldData
	 *            - The value is formated after.
	 */
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
	 * If the value over the specify Length, will join the Error Message.
	 * 
	 * @param currentField
	 *            - Dispose the value need to be formated
	 * @param cutLengthInt
	 *            - The reference for criteria that how length.
	 * @param whichField
	 *            - The reference for Error Message in which field.
	 * @return currentField
	 *            - The value is formated after.
	 */
	//Length Message
	private String handleSingleFiled(String currentField, int cutLengthInt, String whichField) {
		if (currentField.length() <= cutLengthInt) {
			return currentField;
		}
		return currentField + LENGTH_EXCEEDED_DEFAULT_MSG+ "-"+whichField + " ] ";
	}

}

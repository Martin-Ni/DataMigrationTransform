package main.java.com.iisi.corebanking.data.migration;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.iisi.corebanking.data.migration.IllegalCharsetNameException;
import com.iisi.corebanking.data.migration.UnsupportedCharsetException;

public class DataTransformer {
	// Constants for internal setting
	private static final char FIELD_DELIMITER = '|';

	// Property keys (Index of field)
	private static final String UPLOAD_COMPANY_KEY = "upload.company";
	private static final String FIELD_OF_NEW_FILE_KEY = "field.of.new.file";
	
	// Property keys (Error Message)
	
	// Default values (Error Message)
	private final Charset charset;
	private final String lineSeperator;
	private final Properties settings;
	private final Properties settingTOA;
	private final Properties settingCS;
	
	private final String uploadCompanyCode;
	private final ArrayList<String> fileOfNewFileList;
	
	/**
	 * Create an entity of Data Transformer. When you Output, it will use the default the encoding of newline.  
	 * 
	 * @param charsetName
	 *            - Input the Name of Encoding with file.
	 * @param settings
	 *            - Input the Properties of Setting information
	 * @param settingsMsg
	 *            - Input the Properties of Setting Error Message
	 * @param settingTOA
	 *            - Input the Internal Account List
	 * @param settingCS
	 *            - Input the Clearing Suspence List
	 * @throws IllegalCharsetNameException
	 *             If the given charset name is illegal
	 * @throws IllegalArgumentException
	 *             If the given charsetName is null
	 * @throws UnsupportedCharsetException
	 *             If no support for the named charset is available in this
	 *             instance of the Java virtual machine
	 */
	public DataTransformer(String charsetName, Properties settings, Properties settingsMsg, Properties settingTOA, Properties settingCS) {
		this(Charset.forName(charsetName), settings, settingsMsg, settingTOA, settingCS);
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
	 * @param settingTOA
	 *            - Input the Internal Account List
	 * @param settingCS
	 *            - Input the Clearing Suspence List
	 */
	public DataTransformer(Charset charset, Properties settings, Properties settingsMsg, Properties settingTOA, Properties settingCS) {
		this(charset, System.getProperty("line.separator"), settings, settingsMsg, settingTOA, settingCS);
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
	 * @param settingTOA
	 *            - Input the Internal Account List
	 * @param settingCS
	 *            - Input the Clearing Suspence List
	 */
	public DataTransformer(Charset charset, String outputFileLineSeperator,
			Properties settings, Properties settingsMsg, Properties settingTOA, Properties settingCS) {
		super();
		this.charset = charset;
		this.lineSeperator = outputFileLineSeperator;
		this.settings = settings;
		this.settingTOA = settingTOA;
		this.settingCS = settingCS;
		this.uploadCompanyCode = settingsMsg.getProperty(UPLOAD_COMPANY_KEY);
		this.fileOfNewFileList = new ArrayList<String>();
		init();
	}


	/**
	 * Initialize the variable
	 */
	private void init() {
		String fileOfNewFile = settings.getProperty(FIELD_OF_NEW_FILE_KEY);
		if (!isEmptyString(fileOfNewFile)) {
			addToList(fileOfNewFileList, fileOfNewFile);
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
	private void addToList(ArrayList<String> set, String commaDelimitedIndices) {
		String[] indices = commaDelimitedIndices.split(",");
		for (String idx : indices) {
			try {
				//set.add(Integer.parseInt(idx.trim()));
				set.add(idx.trim());
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
	 * Start checking the value is whether follow the Critical.
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
	public void transform(File inputFile, File outputFile)
			throws IOException, IllegalArgumentException {
		if (inputFile.isDirectory()) {
			throw new IllegalArgumentException(inputFile + " is a Directory");
		}
		if (outputFile.isDirectory()) {
			throw new IllegalArgumentException(outputFile + " is a Directory");
		}

		// Read Data from File
		Path filePath = Paths.get(inputFile.toURI());
		List<String> allLines = Files.readAllLines(filePath, charset);

		// Process Each line
		StringBuilder output = new StringBuilder();
		String holdString = "";
		FunctionKeyApp functionKeyApp = new FunctionKeyApp(settings, FIELD_DELIMITER, lineSeperator, settingTOA, settingCS);
		for (String line : allLines) {
			if (line == null || line.trim().length() == 0 || line.indexOf(FIELD_DELIMITER) < 0) {
				continue;
			}
			
			functionKeyApp.fn_setTheLineList(line);
			for(int i = 0 ; i<fileOfNewFileList.size() ; i++){
				if (i != 0) {
					holdString += FIELD_DELIMITER;
				}

				String configSingleValue = fileOfNewFileList.get(i);
				if((configSingleValue).matches("^[0-9]*$")){
					String onlyInsertField = functionKeyApp.fn_onlyInsertOriginal(configSingleValue);
					holdString += onlyInsertField;
					
				}else if(configSingleValue.equalsIgnoreCase("AC")){
					String acCompanyCodeString = functionKeyApp.fn_acCompanyCode(configSingleValue, uploadCompanyCode);
					holdString += acCompanyCodeString;
					
				} else if (configSingleValue.substring(0,2).equalsIgnoreCase("IN")){
					String insertOtherString = functionKeyApp.fn_insertOther(configSingleValue);
					holdString += insertOtherString;
					
				} else if (configSingleValue.substring(0,2).equalsIgnoreCase("CS")){
					String csString = functionKeyApp.fn_clearingSuspense(configSingleValue, uploadCompanyCode);
					holdString += csString;
					
				} else if (configSingleValue.substring(0,3).equalsIgnoreCase("TOA")){
					String toaJudgeString = functionKeyApp.fn_TOAJudge(configSingleValue, uploadCompanyCode);
					holdString += toaJudgeString;
					
				} else if (configSingleValue.substring(0,3).equalsIgnoreCase("NEG")){
					String negateString = functionKeyApp.fn_negateValue(configSingleValue);
					holdString += negateString;
					
				} else if (configSingleValue.substring(0,4).equalsIgnoreCase("COMB")){
					String combineString = functionKeyApp.fn_combine2FieldValue(configSingleValue);
					holdString += combineString;
					
				} else if (configSingleValue.substring(0,4).equalsIgnoreCase("OTOA")){
					String toaString = functionKeyApp.fn_onlyTOAInsert(configSingleValue, uploadCompanyCode);
					holdString += toaString;
					
				} else if (configSingleValue.substring(0,4).equalsIgnoreCase("BTOA")){
					String toaString = functionKeyApp.fn_branchTOAInsert(configSingleValue);
					holdString += toaString;
					
				} else if (configSingleValue.equalsIgnoreCase("LIMIT")){
					String toaString = functionKeyApp.fn_limitZero(configSingleValue);
					holdString = toaString;
				} else if (configSingleValue.equalsIgnoreCase("CUSTOMER")){
					String toaString = functionKeyApp.fn_CustomerFormat(configSingleValue);
					holdString = toaString;
				} else if (configSingleValue.equalsIgnoreCase("GROUP")){
					String toaString = functionKeyApp.fn_GroupFormat(configSingleValue);
					holdString = toaString;
				} else if (configSingleValue.equalsIgnoreCase("IBGROUP")){
					String toaString = functionKeyApp.fn_IBFormat(configSingleValue);
					holdString = toaString;
				} else if (configSingleValue.equalsIgnoreCase("REPLACE")){
					String toaString = functionKeyApp.fn_ReplaceInterface();
					holdString = toaString;
				} else if (configSingleValue.equalsIgnoreCase("AADEPOSIT")){
					String toaString = functionKeyApp.fn_aaDeposit(configSingleValue);
					holdString = toaString;
				} else {
					holdString += "[Warning21]Cannot work";
					
				}
			}
			
			if (holdString != "" ){
				output.append(holdString + lineSeperator);
				holdString = "";
			} 
		}
		if (fileOfNewFileList.get(0).equalsIgnoreCase("LIMIT")) {
			output.append(functionKeyApp.fn_getLimitZero());
		} else if (fileOfNewFileList.get(0).equalsIgnoreCase("CUSTOMER")) {
			output.append(functionKeyApp.fn_getEmailFormat());
		} else if (fileOfNewFileList.get(0).equalsIgnoreCase("GROUP")) {
			output.append(functionKeyApp.fn_getGroupFormat());
		} else if (fileOfNewFileList.get(0).equalsIgnoreCase("IBGROUP")) {
			output.append(functionKeyApp.fn_getIBFormat());
		} else if (fileOfNewFileList.get(0).equalsIgnoreCase("REPLACE")) {
			output.append(functionKeyApp.fn_getReplaceInterface());
		} else if (fileOfNewFileList.get(0).equalsIgnoreCase("AADEPOSIT")) {
			output.append(functionKeyApp.fn_getaaDeposit());
		}
		
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile))) {
			bw.write(output.toString());
		}
	}

	

}

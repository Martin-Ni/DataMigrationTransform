package main.java.com.iisi.corebanking.data.migration;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

public class DataTransformer {
	// Constants for internal setting
	private static final char FIELD_DELIMITER = '|';

	// Property keys (Index of field)
	private static final String FILE_A_KEY = "file.a.key";
	private static final String FILE_B_KEY = "file.b.key";
	private static final String FIELD_A_CHECK = "field.a";
	private static final String FIELD_B_CHECK = "field.b";
	private static final String CSV_TITLE = "csv.title";
	private static final String FIELD_NUMBER_CHECK = "field.number";
	
	//Property keys (Error Message)
	private static final String MAP_NULL_ERRO_KEY="map.null.erro.msg";
	private static final String MAP_REPEAT_ERRO_KEY="map.repeat.erro.msg";
	
	// Default values (Error Message)
	private static String MAP_NULL_ERRO_DEFAULT = "";
	private static String MAP_REPEAT_ERRO_DEFAULT = "";
	
	private final Charset charset;
	private final String lineSeperator;
	private final Properties settings;
	private final Properties settingsMsg;
	private final String funKey;
	private final String fileKeyA;
	private final String fileKeyB;
	private final ArrayList<Integer> fieldCheckListA;
	private final ArrayList<Integer> fieldCheckListB;
	private final ArrayList<Integer> fieldCheckListNumber;
	private String csvTitle;
	private int lineBCount = 0;
	/**
	 * Create an entity of Data Transformer. When you Output, it will use the default the encoding of newline.  
	 * 
	 * @param charsetName
	 *            - Input the Name of Encoding with file.
	 * @param settings
	 *            - Input the Properties of Setting information
	 * @param settingsMsg
	 *            - Input the Properties of Setting Error Message
	 * @param codits
	 *            - the condition of rule
	 * @throws IllegalCharsetNameException
	 *             If the given charset name is illegal
	 * @throws IllegalArgumentException
	 *             If the given charsetName is null
	 * @throws UnsupportedCharsetException
	 *             If no support for the named charset is available in this
	 *             instance of the Java virtual machine
	 */
	public DataTransformer(String charsetName, Properties settings, Properties settingsMsg, String codits) {
		this(Charset.forName(charsetName), settings, settingsMsg, codits);
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
	 * @param codits
	 *            - the condition of rule
	 */
	public DataTransformer(Charset charset, Properties settings, Properties settingsMsg, String codits) {
		this(charset, System.getProperty("line.separator"), settings, settingsMsg, codits);
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
	 * @param codits
	 *            - the condition of rule
	 */
	public DataTransformer(Charset charset, String outputFileLineSeperator,
			Properties settings, Properties settingsMsg, String codits) {
		super();
		this.charset = charset;
		this.lineSeperator = outputFileLineSeperator;
		this.settings = settings;
		this.settingsMsg = settingsMsg;
		this.funKey = codits;
		this.fileKeyA = settings.getProperty(funKey+FILE_A_KEY).trim();
		this.fileKeyB = settings.getProperty(funKey+FILE_B_KEY).trim();
		this.fieldCheckListA = new ArrayList<Integer>();
		this.fieldCheckListB = new ArrayList<Integer>();
		this.fieldCheckListNumber = new ArrayList<Integer>();
		this.csvTitle = settings.getProperty(funKey+CSV_TITLE);
		init();
	}

	/**
	 * Initialize the variable
	 */
	private void init() {
		MAP_NULL_ERRO_DEFAULT =this.settingsMsg.getProperty(MAP_NULL_ERRO_KEY, MAP_NULL_ERRO_DEFAULT);
		MAP_REPEAT_ERRO_DEFAULT =this.settingsMsg.getProperty(MAP_REPEAT_ERRO_KEY, MAP_REPEAT_ERRO_DEFAULT);
		
		String fieldCheckStringA = settings.getProperty(this.funKey+this.FIELD_A_CHECK);
		String fieldCheckStringB = settings.getProperty(this.funKey+this.FIELD_B_CHECK);
		String fieldCheckStringNumber = settings.getProperty(this.funKey+this.FIELD_NUMBER_CHECK);
		
		if (!isEmptyString(fieldCheckStringA)) {
			addToSet(fieldCheckListA, fieldCheckStringA);
		}

		if (!isEmptyString(fieldCheckStringB)) {
			addToSet(fieldCheckListB, fieldCheckStringB);
		}
		
		if (!isEmptyString(fieldCheckStringNumber)) {
			addToSet(fieldCheckListNumber, fieldCheckStringNumber);
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
	private void addToSet(ArrayList<Integer> list, String commaDelimitedIndices) {
		String[] indices = commaDelimitedIndices.split(",");
		for (String idx : indices) {
			try {
				list.add(Integer.parseInt(idx.trim()));
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
	 * Start to compare the data is the same or not.
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
	public void transform(File inputFileA, File inputFileB, File outputFile)
			throws IOException, IllegalArgumentException {
		if (inputFileA.isDirectory()) {
			throw new IllegalArgumentException(inputFileA + " is a Directory");
		}
		if (inputFileB.isDirectory()) {
			throw new IllegalArgumentException(inputFileB + " is a Directory");
		}
		if (outputFile.isDirectory()) {
			throw new IllegalArgumentException(outputFile + " is a Directory");
		}

		// Read Data from File
		Path filePathA = Paths.get(inputFileA.toURI());
		List<String> allLinesA = Files.readAllLines(filePathA, charset);

		Path filePathB = Paths.get(inputFileB.toURI());
		List<String> allLinesB = Files.readAllLines(filePathB, charset);

		// Process Each line
		StringBuilder output = new StringBuilder();
		String[] csvTitle = this.csvTitle.split(",");
		
		
		int lineACount = 0;
		String processedLine = new String();
		HashMap<String,String> allLinesBMap = fileBMap(allLinesB);
		List<String> lineAList = null;
		List<String> lineBList = null;
		
		
		for (String lineA : allLinesA) {
			lineACount++;
			if (lineA == null || lineA.trim().length() == 0 || lineA.indexOf(FIELD_DELIMITER) < 0) {
				continue;
			}
			processedLine = "";
			boolean checkCorrect =true;
			lineAList = lineSeparete(lineA);
			if (allLinesBMap.containsKey(getPrimaryKey(fileKeyA, lineAList))) {
				//processedLine += lineAList.get(fileKeyA);
				lineBList = lineSeparete(allLinesBMap.get(getPrimaryKey(fileKeyA, lineAList)));
				int a =0;
				for (int countFields = 0 ; countFields < fieldCheckListA.size() ; countFields ++){
					int indexA = fieldCheckListA.get(countFields);
					int indexB = fieldCheckListB.get(countFields);
					String dataA = lineAList.get(indexA).trim();
					String dataB = lineBList.get(indexB).trim();
					
					
					if (fieldCheckListNumber.contains(countFields) && 
							!dataA.equals("") && 
							!dataA.equals(null) &&
							!dataB.equals("") &&
							!dataB.equals(null)){
						if (!getBigDecimalNoPoint(dataA).equals(getBigDecimalNoPoint(dataB))){
							checkCorrect = false;
							processedLine += getPrimaryKey(fileKeyA, lineAList)+ "|"+csvTitle[countFields].trim()+"|"+dataA+"|"+dataB + lineSeperator;
						}
					} else if (!dataA.replace("::", "")
								.replace("!!", "")
								.replace(" ", "")
								.replace("'", "")
								.replace(";", "")
								.replace("\"", "")
								.replace("?", "")
								.replace(",", "")
							.equals(dataB.replace("::", "")
											.replace("!!", "")
											.replace(" ", "")
											.replace("'", "")
											.replace(";", "")
											.replace("\"", "")
											.replace("?", "")
											.replace(",", ""))){
						checkCorrect = false;
						processedLine += getPrimaryKey(fileKeyA, lineAList)+ "|"+csvTitle[countFields].trim()+"|"+dataA+"|"+dataB + lineSeperator;
					}
					System.out.println(a++);
				}
				allLinesBMap.remove(getPrimaryKey(fileKeyA, lineAList));
				if (checkCorrect == true){
					processedLine = "";
				}
				
			} else {
				processedLine += getPrimaryKey(fileKeyA, lineAList)+" "+funKey.split("\\.")[0]+","+MAP_NULL_ERRO_DEFAULT+lineSeperator;
			}
			
			output.append(processedLine);
			System.out.println(lineACount);
			
		}
		output.append(lineSeperator + lineSeperator + funKey.split("\\.")[0]+ " Total Record |" +lineACount);
		output.append(lineSeperator + funKey.split("\\.")[1]+ " Total Record |" +this.lineBCount);
		output.append(lineSeperator + lineSeperator + "Below is No use in File " + funKey.split("\\.")[1] + lineSeperator);
		
		for (String remainList : allLinesBMap.keySet()) {
			output.append(remainList);
			output.append(lineSeperator);
		}
		
		// Write to output file
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile))) {
			bw.write(output.toString());
		}
	}

	/**
	 * Separate routine
	 * 
	 * @param line
	 *            - one Record String.
	 * @return mapping
	 *            - Return the record Separate List .
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
	 * Slit the File B Reocord to HashMap
	 * 
	 * @param allLines
	 *            - All the Record List.
	 * @return mapping
	 *            - Return the Hashmap list.
	 */
	private HashMap<String, String> fileBMap(List<String> allLines) {
		HashMap<String, String> mapping = new HashMap<String, String>();
		int lineBCount = 0;
		for (String line : allLines) {
			lineBCount++;
			if (line == null || line.trim().length() == 0 || line.indexOf(FIELD_DELIMITER) < 0) {
				continue;
			}
			List<String> lineStringList = lineSeparete(line);
			if (mapping.containsKey(getPrimaryKey(fileKeyB, lineStringList))) {
				mapping.put(getPrimaryKey(fileKeyB, lineStringList)+" "+lineBCount+", ", MAP_REPEAT_ERRO_DEFAULT);
			} else {
				mapping.put(getPrimaryKey(fileKeyB, lineStringList), line);
			}
		}
		this.lineBCount = lineBCount;
		return mapping;
	}
	
	/**
	 * Setting the Primary Key for Reference
	 * 
	 * @param indexString
	 *            - The reference which fields combine to one Primary Key.
	 * @param lineStringList
	 *            - The value list in one record.
	 * @return primaryKey
	 *            - Return the primary key.
	 **/
	private String getPrimaryKey(String indexString, List<String> lineStringList) {
		String[] indexArray = indexString.split(",");
		String primaryKey = "";
		for (int seq = 0 ; seq < indexArray.length ; seq++ ){
			if (seq != 0){
				primaryKey += "?";
			}
			primaryKey += lineStringList.get(Integer.parseInt(indexArray[seq].trim()));
		}
		return primaryKey;
	}
	
	/**
	 * Format to two decimal numeric for compare. 
	 * 
	 * @param moneyInput
	 *            - The numeric value.
	 * @return moneyOutput
	 *            - Return the numeric value of two decimal.
	 **/
	private BigDecimal getBigDecimalNoPoint(String moneyInput){
		BigDecimal moneyOutput = new BigDecimal (moneyInput).setScale(2,BigDecimal.ROUND_HALF_UP);
		return moneyOutput;
	}
}

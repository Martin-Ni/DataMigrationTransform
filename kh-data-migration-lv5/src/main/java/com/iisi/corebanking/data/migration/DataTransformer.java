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
	 * �̷өҵ��w���]�w�إߤ@��DataTransformer����C��X�ɮױN�|�ϥΨt�ιw�]������r���C
	 * 
	 * @param charsetName
	 *            - ��J�ɮת��s�X�W��
	 * @param settings
	 *            - �t���]�w��T��Properties
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
	 * �̷өҵ��w���]�w�إߤ@��DataTransformer����C��X�ɮױN�|�ϥΨt�ιw�]������r���C
	 * 
	 * @param charset
	 *            - ��J�ɮת��s�X
	 * @param settings
	 *            - �t���]�w��T��Properties
	 */
	public DataTransformer(Charset charset, Properties settings, Properties settingsMsg, String codits) {
		this(charset, System.getProperty("line.separator"), settings, settingsMsg, codits);
	}

	/**
	 * �̷өҵ��w���]�w�إߤ@��DataTransformer����
	 * 
	 * @param charset
	 *            - ��J�ɮת��s�X
	 * @param outputFileLineSeperator
	 *            - ��X�ɮשҭn�ϥΪ�����r��
	 * @param settings
	 *            - �t���]�w��T��Properties
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
	 * ��l�ƹ����ܼ�
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
	 * �N�H�r�����j���h�������ޤ��Ψ�trim���ରInteger�[�JSet�C �Y�Ҥ��ΥX�Ӫ��r��L�k�Q�ഫ��Integer�A�h�|���L�ӵ���ơC
	 * 
	 * @param set
	 *            - �n�[�J��ƪ�Set
	 * @param commaDelimitedIndices
	 *            - �r�����Ϊ�������
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
	 * �P�_�r��O�_�L�N�q
	 * 
	 * @param string
	 *            - �n�P�_���r��
	 * @return �P�_���G�A�Y string != null || string.trim().length() == 0
	 */
	private boolean isEmptyString(String string) {
		return string == null || string.trim().length() == 0;
	}

	/**
	 * �ഫ�ɮפ��e
	 * 
	 * @param inputFile
	 *            - ��ƿ�J�ӷ��ɮ�
	 * @param outputFile
	 *            - ��ƿ�X�ت��ɮ�
	 * @throws IOException
	 *             �p�GIO�o�Ϳ��~
	 * @throws IllegalArgumentException
	 *             �ҵ��w�����@�ѼƤ����V����Ƨ��ӫD�ɮ�
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
		output.append(lineSeperator + lineSeperator + "Below is No use in File " + funKey.split("\\.")[1] + lineSeperator + allLinesBMap);
		
		
		
		// Write to output file
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile))) {
			bw.write(output.toString());
		}
	}

	// ���}
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
	
	// File B ���}�æs�JHashMap
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
	
	private BigDecimal getBigDecimalNoPoint(String moneyInput){
		BigDecimal moneyOutput = new BigDecimal (moneyInput).setScale(2,BigDecimal.ROUND_HALF_UP);
		return moneyOutput;
	}
	

	// �X��
	private String lineMerge(List<String> lineList) {
		StringBuilder lineMerge = new StringBuilder();
		for (int i = 0; i < lineList.size(); i++) {
			if (i != 0) {
				lineMerge.append(FIELD_DELIMITER);
			}
			lineMerge.append(lineList.get(i));
		}
		return lineMerge.toString();
	}
}

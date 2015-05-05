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
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

public class DataTransformer {
	// Constants for internal setting
	private static final char FIELD_DELIMITER = '|';

	// Property keys (Index of field)
	private static final String FILE_A_MAP_FIELD_KEY = "field.a.map";
	private static final String FILE_B_MAP_FIELD_KEY = "field.b.map";
	private static final String FILE_A_INSERT_FIELD_KEY = "field.a.insert";
	private static final String FILE_B_EXTRACT_FIELD_KEY = "field.b.extract";
	
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
	private final ArrayList<Integer> mapFieldSetA;
	private final ArrayList<Integer> mapFieldSetB;
	private final ArrayList<Integer> mapFieldInsertSetA;
	private final ArrayList<Integer> mapFieldExtractSetB;	
	
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
	public DataTransformer(String charsetName, Properties settings, Properties settingsMsg) {
		this(Charset.forName(charsetName), settings, settingsMsg);
	}

	/**
	 * �̷өҵ��w���]�w�إߤ@��DataTransformer����C��X�ɮױN�|�ϥΨt�ιw�]������r���C
	 * 
	 * @param charset
	 *            - ��J�ɮת��s�X
	 * @param settings
	 *            - �t���]�w��T��Properties
	 */
	public DataTransformer(Charset charset, Properties settings, Properties settingsMsg) {
		this(charset, System.getProperty("line.separator"), settings, settingsMsg);
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
			Properties settings, Properties settingsMsg) {
		super();
		this.charset = charset;
		this.lineSeperator = outputFileLineSeperator;
		this.settings = settings;
		this.settingsMsg = settingsMsg;
		this.mapFieldSetA = new ArrayList<Integer>();
		this.mapFieldSetB = new ArrayList<Integer>();
		this.mapFieldInsertSetA = new ArrayList<Integer>();
		this.mapFieldExtractSetB = new ArrayList<Integer>();
		init();
	}

	/**
	 * ��l�ƹ����ܼ�
	 */
	private void init() {
		MAP_NULL_ERRO_DEFAULT =this.settingsMsg.getProperty(MAP_NULL_ERRO_KEY, MAP_NULL_ERRO_DEFAULT);
		MAP_REPEAT_ERRO_DEFAULT =this.settingsMsg.getProperty(MAP_REPEAT_ERRO_KEY, MAP_REPEAT_ERRO_DEFAULT);
		
		String mapFieldA = settings.getProperty(FILE_A_MAP_FIELD_KEY);
		String mapFieldB = settings.getProperty(FILE_B_MAP_FIELD_KEY);
		String mapFieldInsertA = settings.getProperty(FILE_A_INSERT_FIELD_KEY);
		String mapFieldExtractB = settings.getProperty(FILE_B_EXTRACT_FIELD_KEY);

		if (!isEmptyString(mapFieldA)) {
			addToSet(mapFieldSetA, mapFieldA);
		}
		
		if (!isEmptyString(mapFieldB)) {
			addToSet(mapFieldSetB, mapFieldB);
		}
		
		if (!isEmptyString(mapFieldInsertA)) {
			addToSet(mapFieldInsertSetA, mapFieldInsertA);
		}

		if (!isEmptyString(mapFieldExtractB)) {
			addToSet(mapFieldExtractSetB, mapFieldExtractB);
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
	private void addToSet(ArrayList<Integer> set, String commaDelimitedIndices) {
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
		boolean notFirst = false;
		int count = 0;
		String processedLine = new String();
		HashMap<String,String> allLinesBMap = fileBMap(allLinesB);
		List<String> lineAList = null;
		
		for (String lineA : allLinesA) {
			if (lineA == null || lineA.trim().length() == 0 || lineA.indexOf(FIELD_DELIMITER) < 0) {
				continue;
			}
			lineAList = lineSeparete(lineA);
			
			
			if (allLinesBMap.containsKey(lineAList.get(mapFieldSetA.get(0)))) {
				lineAList.set(mapFieldInsertSetA.get(0),allLinesBMap.get(lineAList.get(mapFieldSetA.get(0))));
				allLinesBMap.remove(lineAList.get(mapFieldSetA.get(0)));
				//allLinesBMap.put(lineAList.get(mapFieldSetA.get(0)),"warning6");
			} else {
				//lineAList.set(mapFieldInsertSetA.get(0),allLinesBMap.get(lineAList.get(mapFieldSetA.get(0)))+MAP_NULL_ERRO_DEFAULT);//value will be replace by null
				lineAList.set(mapFieldInsertSetA.get(0),lineAList.get(mapFieldInsertSetA.get(0))+MAP_NULL_ERRO_DEFAULT);//value will keep the original value
			}
			
			processedLine = lineMerge(lineAList);
			if (notFirst) {
				output.append(lineSeperator);
			} else {
				notFirst = true;
			}
			output.append(processedLine);
			count++;
			System.out.println(count);
		}
		output.append(lineSeperator+allLinesBMap);
		
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
		for (String line : allLines) {
			if (line == null || line.trim().length() == 0 || line.indexOf(FIELD_DELIMITER) < 0) {
				continue;
			}
			List<String> lineStringList = lineSeparete(line);
			if (mapping.containsKey(lineStringList.get(mapFieldSetB.get(0)))) {
				mapping.put(lineStringList.get(mapFieldSetB.get(0)), lineStringList.get(mapFieldExtractSetB.get(0))+MAP_REPEAT_ERRO_DEFAULT);
			} else {
				mapping.put(lineStringList.get(mapFieldSetB.get(0)), lineStringList.get(mapFieldExtractSetB.get(0)));
			}
		}
		return mapping;
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

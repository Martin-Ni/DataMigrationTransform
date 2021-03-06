package main.java.com.iisi.corebanking.data.migration;

import java.awt.Color;
import java.awt.EventQueue;

import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.border.EmptyBorder;
import javax.swing.JButton;
import javax.swing.JFileChooser;
import javax.swing.JOptionPane;
import javax.swing.JScrollPane;
import javax.swing.JTextField;
import javax.swing.JLabel;
import javax.swing.SwingConstants;
import javax.swing.JSeparator;

import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import javax.swing.JTextArea;
import java.awt.Font;

public class MainFrame extends JFrame {

	private static final long serialVersionUID = 1L;

	private JPanel contentPane;
	private JButton btnOpenSourceFolder_CUBC;
	private JButton btnOpenSourceFolder_IISI;
	private JButton btnOpenSourceFolder_T24;
	private JScrollPane scrollPane;
	private JButton btnOpenConfigFolder;
	private JTextField txtPrefix;
	private JLabel lblIgnoredPrefix;
	private JLabel label;
	private JButton btnConvert;
	private JButton btnChooseOutputFolder;
	private JButton btnSaveSetting;
	private JButton btnCreateSettingMsg;
	private JSeparator separator;
	private JFileChooser chooserIn_CUBC;
	private JFileChooser chooserIn_IISI;
	private JFileChooser chooserIn_T24;
	private JFileChooser chooserConfig;
	private JFileChooser chooserOut;
	private JTextArea ta;

	private JLabel lblNewLabel;
	private JTextField txtCharset;
	private JLabel lblIgnoringFilenameSuffix;
	private JLabel label_2;
	private JTextField txtSuffix;
	private JLabel lblInputFileSuffix;
	private JTextField txtInSuffix;
	private JLabel lblConfigFilenameSuffix;
	private JTextField txtConfigSuffix;
	private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");

	/**
	 * Launch the application.
	 */
	public static void main(String[] args) {
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					MainFrame frame = new MainFrame();
					frame.setVisible(true);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

	/**
	 * Create the frame.
	 */
	public MainFrame() {
		// Initialize JFileChoosers
		File f = new File(".");
		chooserIn_CUBC = new JFileChooser(".");
		chooserIn_IISI = new JFileChooser(".");
		chooserIn_T24 = new JFileChooser(".");
		chooserConfig = new JFileChooser(".");
		chooserOut = new JFileChooser(".");
		chooserIn_CUBC.setSelectedFile(f);
		chooserIn_IISI.setSelectedFile(f);
		chooserIn_T24.setSelectedFile(f);
		chooserConfig.setSelectedFile(f);
		chooserOut.setSelectedFile(f);
		chooserIn_CUBC.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
		chooserIn_IISI.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
		chooserIn_T24.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
		chooserConfig.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
		chooserOut.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);

		setTitle("IISI CUBKH Data Migration Utility Level 3 Data Extract and Insert");
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		setBounds(100, 100, 694, 487);
		contentPane = new JPanel();
		contentPane.setBorder(new EmptyBorder(5, 5, 5, 5));
		//Color mycolor = new Color(94, 255, 0).darker();
		contentPane.setBackground(Color.LIGHT_GRAY);
		setContentPane(contentPane);
		contentPane.setLayout(null);

		btnOpenSourceFolder_CUBC = new JButton("Open Source File CUBC");
		btnOpenSourceFolder_CUBC.setBounds(10, 10, 170, 23);
		contentPane.add(btnOpenSourceFolder_CUBC);

		btnOpenSourceFolder_IISI = new JButton("Open Source File IISI");
		btnOpenSourceFolder_IISI.setBounds(10, 40, 170, 23);
		contentPane.add(btnOpenSourceFolder_IISI);
		
		btnOpenSourceFolder_T24 = new JButton("Open Source File T24");
		btnOpenSourceFolder_T24.setBounds(10, 70, 170, 23);
		contentPane.add(btnOpenSourceFolder_T24);
		
		
		scrollPane = new JScrollPane();
		scrollPane.setBounds(10, 216, 658, 232);
		contentPane.add(scrollPane);

		ta = new JTextArea();
		ta.setText("Source Folder, Config Folder and Output Folder are defaulted to current directory (\".\").\n");
		scrollPane.setViewportView(ta);

		btnOpenConfigFolder = new JButton("Open Config File");
		btnOpenConfigFolder.setBounds(10, 100, 170, 23);
		contentPane.add(btnOpenConfigFolder);

		txtPrefix = new JTextField();
		txtPrefix.setBounds(246, 31, 422, 21);
		contentPane.add(txtPrefix);
		txtPrefix.setColumns(10);

		lblIgnoredPrefix = new JLabel("Ignoring Filename Prefix (RegEx, starts with ^):");
		lblIgnoredPrefix.setToolTipText("Specify the filename prefix that will not present in the filename of config file");
		lblIgnoredPrefix.setBounds(225, 10, 328, 15);
		contentPane.add(lblIgnoredPrefix);

		label = new JLabel("^");
		label.setHorizontalAlignment(SwingConstants.RIGHT);
		label.setBounds(225, 34, 14, 15);
		contentPane.add(label);

		btnConvert = new JButton("Convert !");
		btnConvert.setBounds(10, 165, 170, 30);
		contentPane.add(btnConvert);

		btnChooseOutputFolder = new JButton("Choose Output Folder");
		btnChooseOutputFolder.setBounds(10, 130, 170, 23);
		contentPane.add(btnChooseOutputFolder);

		btnCreateSettingMsg = new JButton("Create Error");
		btnCreateSettingMsg.setBounds(557, 115, 111, 23);
		contentPane.add(btnCreateSettingMsg);
		
		btnSaveSetting = new JButton("Save settings");
		btnSaveSetting.setBounds(557, 144, 111, 23);
		contentPane.add(btnSaveSetting);

		separator = new JSeparator();
		separator.setBounds(10, 159, 170, 9);
		contentPane.add(separator);

		lblNewLabel = new JLabel("Input Charset:");
		lblNewLabel.setBounds(210, 124, 88, 15);
		contentPane.add(lblNewLabel);

		txtCharset = new JTextField();
		txtCharset.setText("UTF-8");
		txtCharset.setBounds(210, 145, 88, 21);
		txtCharset.setColumns(10);
		contentPane.add(txtCharset);

		lblIgnoringFilenameSuffix = new JLabel("Ignoring Filename Suffix (RegEx, ends with $):");
		lblIgnoringFilenameSuffix.setToolTipText("Specify the filename prefix that will not present in the filename of config file");
		lblIgnoringFilenameSuffix.setBounds(225, 62, 328, 15);
		contentPane.add(lblIgnoringFilenameSuffix);

		label_2 = new JLabel("$");
		label_2.setHorizontalAlignment(SwingConstants.LEFT);
		label_2.setBounds(660, 92, 14, 15);
		contentPane.add(label_2);

		txtSuffix = new JTextField();
		txtSuffix.setText("\\.txt");
		txtSuffix.setColumns(10);
		txtSuffix.setBounds(225, 87, 428, 21);
		contentPane.add(txtSuffix);

		lblInputFileSuffix = new JLabel("Input Filename suffix:");
		lblInputFileSuffix.setFont(new Font("Tahoma", Font.PLAIN, 11));
		lblInputFileSuffix.setBounds(308, 124, 119, 15);
		contentPane.add(lblInputFileSuffix);

		txtInSuffix = new JTextField();
		txtInSuffix.setText(".txt");
		txtInSuffix.setColumns(10);
		txtInSuffix.setBounds(308, 145, 119, 21);
		contentPane.add(txtInSuffix);

		lblConfigFilenameSuffix = new JLabel("Config Filename suffix:");
		lblConfigFilenameSuffix.setFont(new Font("Tahoma", Font.PLAIN, 11));
		lblConfigFilenameSuffix.setBounds(434, 124, 119, 15);
		contentPane.add(lblConfigFilenameSuffix);

		txtConfigSuffix = new JTextField();
		txtConfigSuffix.setText(".properties");
		txtConfigSuffix.setColumns(10);
		txtConfigSuffix.setBounds(434, 145, 119, 21);
		contentPane.add(txtConfigSuffix);

		initBtns();
		loadSave();
		printFolderPaths();
	}

	/**
	 * Initializes The Button of Function.
	 */
	private void initBtns() {
		// Open Source Folder CUBC
		btnOpenSourceFolder_CUBC.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				if (chooserIn_CUBC.showOpenDialog(MainFrame.this) == JFileChooser.APPROVE_OPTION) {
					ta.setText("Data Input Directory: " + chooserIn_CUBC.getSelectedFile().toString());
				}
			}
		});

		// Open Source Folder IISI
		btnOpenSourceFolder_IISI.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				if (chooserIn_IISI.showOpenDialog(MainFrame.this) == JFileChooser.APPROVE_OPTION) {
					ta.setText("Data Input Directory: " + chooserIn_IISI.getSelectedFile().toString());
				}
			}
		});
		
		// Open Source Folder T24
		btnOpenSourceFolder_T24.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				if (chooserIn_T24.showOpenDialog(MainFrame.this) == JFileChooser.APPROVE_OPTION) {
					ta.setText("Data Input Directory: " + chooserIn_T24.getSelectedFile().toString());
				}
			}
		});

		// Open Config Folder
		btnOpenConfigFolder.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				if (chooserConfig.showOpenDialog(MainFrame.this) == JFileChooser.APPROVE_OPTION) {
					ta.setText("Config File Directory: " + chooserConfig.getSelectedFile().toString());
				}
			}
		});

		// Open Output Folder
		btnChooseOutputFolder.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				if (chooserOut.showOpenDialog(MainFrame.this) == JFileChooser.APPROVE_OPTION) {
					ta.setText("Output File Directory: " + chooserOut.getSelectedFile().toString());
				}
			}
		});

		// Convert !
		btnConvert.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				ta.setText("**** Start Convert Data ****\n\n");
				try {
					File[] fs_CUBC = chooserIn_CUBC.getSelectedFile().listFiles(new FileNameSuffixFilter(txtInSuffix.getText()));
					ta.append("Listing file(s) under folder [ " + chooserIn_CUBC.getSelectedFile() + " ]\n");
					ta.append("Got " + fs_CUBC.length + " candidate file(s)\n");
					
					File[] fs_IISI = chooserIn_IISI.getSelectedFile().listFiles(new FileNameSuffixFilter(txtInSuffix.getText()));
					ta.append("Listing file(s) under folder [ " + chooserIn_IISI.getSelectedFile() + " ]\n");
					ta.append("Got " + fs_IISI.length + " candidate file(s)\n");
					
					File[] fs_T24 = chooserIn_T24.getSelectedFile().listFiles(new FileNameSuffixFilter(txtInSuffix.getText()));
					ta.append("Listing file(s) under folder [ " + chooserIn_T24.getSelectedFile() + " ]\n");
					ta.append("Got " + fs_T24.length + " candidate file(s)\n");

					String charsetName = txtCharset.getText();
					ta.append("Reading files using charset \"" + charsetName + "\"\n\n");
					
					Properties settingMsg = new Properties();
					try (FileReader frMsg = new FileReader("./saveMsgLv5.properties");) {
						settingMsg.load(frMsg);
						frMsg.close();
						ta.append("Messages Setting Loaded \n\n");
					}catch (Exception e1) {
						System.out.println("saveMsgLv5 Load canceled");
						// load canceled
					}
					
					//String codit ="CUBC.IISI";//, IISI_T24, CUBC_T24";
					
					String[] coditFunction= settingMsg.getProperty("match.condition.setting").split(",");
					
					for(String codits : coditFunction){
							
							File[] fs_A = null;
							switch(codits){
								case "CUBC.IISI.":
									fs_A = fs_CUBC;
									break;
									
								case "IISI.T24.":
									fs_A = fs_IISI;
									break;
									
								case "CUBC.T24.":
									fs_A = fs_CUBC;
									break;
								case "IISI.CUBC.":
									fs_A = fs_IISI;
									break;
									
								case "T24.IISI.":
									fs_A = fs_T24;
									break;
									
								case "T24.CUBC.":
									fs_A = fs_T24;
									break;
								
							}
							
							for (File f_A : fs_A){
								ta.append("Read file [ " + f_A + " ] By insert \n");
								String prefixRegex = "^" + txtPrefix.getText();
								String suffixRegex = txtSuffix.getText() + "$";
								String configFileName = f_A.getName().replaceFirst(prefixRegex, "").replaceFirst(suffixRegex, "") + txtConfigSuffix.getText();
								String FileNameB = f_A.getName().replaceFirst(prefixRegex, "");
								ta.append("Resolved config filename is [ " + configFileName + " ] under [ " + chooserConfig.getSelectedFile() + " ]\n");
								
								File f_B = null;
								switch(codits){
									case "CUBC.IISI.":
										f_B = new File(chooserIn_IISI.getSelectedFile(), FileNameB);
										break;
										
									case "IISI.T24.":
										f_B = new File(chooserIn_T24.getSelectedFile(), FileNameB);
										break;
										
									case "CUBC.T24.":
										f_B = new File(chooserIn_T24.getSelectedFile(), FileNameB);
										break;
									case "IISI.CUBC.":
										f_B = new File(chooserIn_CUBC.getSelectedFile(), FileNameB);
										break;
										
									case "T24.IISI.":
										f_B = new File(chooserIn_IISI.getSelectedFile(), FileNameB);
										break;
										
									case "T24.CUBC.":
										f_B = new File(chooserIn_CUBC.getSelectedFile(), FileNameB);
										break;
										
								}
								
								Properties setting = new Properties();
								FileReader fr = new FileReader(new File(chooserConfig.getSelectedFile(), configFileName));
								setting.load(fr);
								fr.close();
								
								// Getting the File Name of Output
								File outputFile = new File(chooserOut.getSelectedFile(), codits+f_A.getName());
								if (outputFile.exists()) {
									String newFileName = dateFormat.format(new Date()) + " " + codits + f_A.getName();
									outputFile = new File(chooserOut.getSelectedFile(), newFileName);
								}
								ta.append("Output file: " + outputFile + "\n");
								// Start to transfer the Data.
								ta.append("Initialize DataTransformer\n");
								DataTransformer transformer = new DataTransformer(charsetName, setting, settingMsg, codits);
								transformer.transform(f_A, f_B, outputFile);
								ta.append("Data transform completed");
								ta.append("\n--\n");
							}

					}
				} catch (Exception ex) {
					ex.printStackTrace();
					printException(ex);
				}
				ta.append("**** End Convert Data ****\n");
			}
		});

		// Save Settings
		btnSaveSetting.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				Properties props = new Properties();
				props.setProperty("dir.in_CUBC", chooserIn_CUBC.getSelectedFile().toString());
				props.setProperty("dir.in_IISI", chooserIn_IISI.getSelectedFile().toString());
				props.setProperty("dir.in_T24", chooserIn_T24.getSelectedFile().toString());
				props.setProperty("dir.out", chooserOut.getSelectedFile().toString());
				props.setProperty("dir.config", chooserConfig.getSelectedFile().toString());
				props.setProperty("txt.prefix", txtPrefix.getText());
				props.setProperty("txt.suffix", txtSuffix.getText());
				props.setProperty("txt.charset", txtCharset.getText());
				props.setProperty("txt.inFilename.suffix", txtInSuffix.getText());
				props.setProperty("txt.configFilename.suffix", txtConfigSuffix.getText());

				try (FileWriter fw = new FileWriter("saveLv5.properties");) {
					props.store(fw, new Date().toString());
					JOptionPane.showMessageDialog(MainFrame.this, "Saved", "Notification", JOptionPane.INFORMATION_MESSAGE);
				} catch (IOException e1) {
					e1.printStackTrace();
					printException(e1);
				}
			}
		});
		
		btnCreateSettingMsg.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				Properties props = new Properties();
				props.setProperty("map.null.erro.msg", "[Warning31]ID of file A doesn't have the value in file B");
				props.setProperty("map.repeat.erro.msg", "[Warning32]ID is repeat in file B");
				
				try (FileWriter fw = new FileWriter("saveMsgLv5.properties");) {
					props.store(fw, new Date().toString());
					JOptionPane.showMessageDialog(MainFrame.this, "Create the Default Error Message!", "Notification", JOptionPane.INFORMATION_MESSAGE);
				} catch (IOException e1) {
					e1.printStackTrace();
					printException(e1);
				}
			}
		});
	}


	private void loadSave() {
		try (FileReader fr = new FileReader("./saveLv5.properties");) {
			Properties props = new Properties();
			props.load(fr);
			String inDir_CUBC = props.getProperty("dir.in_CUBC");
			if (inDir_CUBC != null) {
				chooserIn_CUBC.setSelectedFile(new File(inDir_CUBC));
			}
			
			String inDir_IISI = props.getProperty("dir.in_IISI");
			if (inDir_IISI != null) {
				chooserIn_IISI.setSelectedFile(new File(inDir_IISI));
			}
			
			String inDir_T24 = props.getProperty("dir.in_T24");
			if (inDir_T24 != null) {
				chooserIn_T24.setSelectedFile(new File(inDir_T24));
			}
			
			String outDir = props.getProperty("dir.out");
			if (outDir != null) {
				chooserOut.setSelectedFile(new File(outDir));
			}
			String configDir = props.getProperty("dir.config");
			if (configDir != null) {
				chooserConfig.setSelectedFile(new File(configDir));
			}
			String prefix = props.getProperty("txt.prefix");
			if (prefix != null) {
				txtPrefix.setText(prefix);
			}
	
			String suffix = props.getProperty("txt.suffix");
			if (suffix != null) {
				txtSuffix.setText(suffix);
			}
			String charset = props.getProperty("txt.charset");
			if (charset != null) {
				txtCharset.setText(charset);
			}
			String infs = props.getProperty("txt.inFilename.suffix");
			if (infs != null) {
				txtInSuffix.setText(infs);
			}
			String confs = props.getProperty("txt.configFilename.suffix");
			if (confs != null) {
				txtConfigSuffix.setText(confs);
			}
			ta.setText("Settings loaded\n");
			
		} catch (Exception e1) {
			System.out.println("Load canceled");
			// load canceled
		}
		ta.append("This is the Data Migratin in CUB Cambodia \n "
				+ "================= Level 5 ================= \n"
				+ "Data Migration Reconsolidation \n"
				+ ""
				+ ""
				+ "==========================V1.0.20150708==== \n");
	}

	/**
	 * Display the Path of Folder that be choose in the View.
	 */
	private void printFolderPaths() {
		ta.append("\n");
		ta.append("Data Input Directory CUBC: " + chooserIn_CUBC.getSelectedFile().getAbsolutePath());
		ta.append("\n");
		ta.append("Data Input Directory IISI: " + chooserIn_IISI.getSelectedFile().getAbsolutePath());
		ta.append("\n");
		ta.append("Data Input Directory T24 " + chooserIn_T24.getSelectedFile().getAbsolutePath());
		ta.append("\n");
		ta.append("Config File Directory: " + chooserConfig.getSelectedFile().getAbsolutePath());
		ta.append("\n");
		ta.append("Output File Directory: " + chooserOut.getSelectedFile().getAbsolutePath());
	}
	/**
	 * Display the Exception on the View.
	 * @param e - Output Exception
	 */
	private void printException(Exception e) {
		ta.append("\n");
		ta.append(e.toString());
		for (StackTraceElement se : e.getStackTrace()) {
			ta.append("\n  ");
			ta.append(se.toString());
		}
	}

	private class FileNameSuffixFilter implements java.io.FileFilter {
		private String suffix;

		/**
		 * @param suffix
		 */
		public FileNameSuffixFilter(String suffix) {
			super();
			this.suffix = suffix;
		}

		@Override
		public boolean accept(File pathname) {
			if (pathname.getName().endsWith(suffix) && !pathname.isDirectory())
				return true;
			return false;
		}
	}
}

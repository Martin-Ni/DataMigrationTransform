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
	private JButton btnOpenSourceFolder;
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
	private JFileChooser chooserIn;
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
		chooserIn = new JFileChooser(".");
		chooserConfig = new JFileChooser(".");
		chooserOut = new JFileChooser(".");
		chooserIn.setSelectedFile(f);
		chooserConfig.setSelectedFile(f);
		chooserOut.setSelectedFile(f);
		chooserIn.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
		chooserConfig.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
		chooserOut.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);

		setTitle("IISI CUBKH Data Migration Utility Level 0");
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		setBounds(100, 100, 694, 457);
		contentPane = new JPanel();
		contentPane.setBorder(new EmptyBorder(5, 5, 5, 5));
		contentPane.setBackground(Color.DARK_GRAY);
		setContentPane(contentPane);
		contentPane.setLayout(null);

		btnOpenSourceFolder = new JButton("Open Source Folder");
		btnOpenSourceFolder.setBounds(10, 10, 170, 23);
		contentPane.add(btnOpenSourceFolder);

		scrollPane = new JScrollPane();
		scrollPane.setBounds(10, 176, 658, 232);
		contentPane.add(scrollPane);

		ta = new JTextArea();
		ta.setText("Source Folder, Config Folder and Output Folder are defaulted to current directory (\".\").\n");
		scrollPane.setViewportView(ta);

		btnOpenConfigFolder = new JButton("Open Config Folder");
		btnOpenConfigFolder.setBounds(10, 43, 170, 23);
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
		btnConvert.setBounds(10, 124, 170, 42);
		contentPane.add(btnConvert);

		btnChooseOutputFolder = new JButton("Choose Output Folder");
		btnChooseOutputFolder.setBounds(10, 76, 170, 23);
		contentPane.add(btnChooseOutputFolder);

		btnCreateSettingMsg = new JButton("Create Error");
		btnCreateSettingMsg.setBounds(557, 115, 111, 23);
		contentPane.add(btnCreateSettingMsg);
		
		btnSaveSetting = new JButton("Save settings");
		btnSaveSetting.setBounds(557, 144, 111, 23);
		contentPane.add(btnSaveSetting);

		separator = new JSeparator();
		separator.setBounds(10, 109, 170, 9);
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
		// Open Source Folder
		btnOpenSourceFolder.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				if (chooserIn.showOpenDialog(MainFrame.this) == JFileChooser.APPROVE_OPTION) {
					ta.setText("Data Input Directory: " + chooserIn.getSelectedFile().toString());
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
					File[] fs = chooserIn.getSelectedFile().listFiles(new FileNameSuffixFilter(txtInSuffix.getText()));
					ta.append("Listing file(s) under folder [ " + chooserIn.getSelectedFile() + " ]\n");
					ta.append("Got " + fs.length + " candidate file(s)\n");

					String charsetName = txtCharset.getText();
					ta.append("Reading files using charset \"" + charsetName + "\"\n\n");

					Properties settingMsg = new Properties();
					try (FileReader frMsg = new FileReader("./saveMsgLv0.properties");) {
						settingMsg.load(frMsg);
						frMsg.close();
						ta.append("Messages Setting Loaded \n\n");
					}catch (Exception e1) {
						System.out.println("saveMsgLv0 Load canceled");
						// load canceled
					}
					

					for (File f : fs) {
						ta.append("Read file [ " + f + " ]\n");
						// Loading the Setting File
						String prefixRegex = "^" + txtPrefix.getText();
						String suffixRegex = txtSuffix.getText() + "$";
						String configFileName = f.getName().replaceFirst(prefixRegex, "").replaceFirst(suffixRegex, "") + txtConfigSuffix.getText();
						ta.append("Resolved config filename is [ " + configFileName + " ] under [ " + chooserConfig.getSelectedFile() + " ]\n");
						Properties setting = new Properties();
						FileReader fr = new FileReader(new File(chooserConfig.getSelectedFile(), configFileName));
						setting.load(fr);
						fr.close();
						// Getting the File Name of Output
						File outputFile = new File(chooserOut.getSelectedFile(), f.getName());
						if (outputFile.exists()) {
							String newFileName = dateFormat.format(new Date()) + " " + f.getName();
							outputFile = new File(chooserOut.getSelectedFile(), newFileName);
						}
						ta.append("Output file: " + outputFile + "\n");
						// Start to transfer the Data.
						ta.append("Initialize DataTransformer\n");
						DataTransformer transformer = new DataTransformer(charsetName, setting, settingMsg);
						transformer.transform(f, outputFile);
						ta.append("Data transform completed");
						ta.append("\n--\n");
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
				props.setProperty("dir.in", chooserIn.getSelectedFile().toString());
				props.setProperty("dir.out", chooserOut.getSelectedFile().toString());
				props.setProperty("dir.config", chooserConfig.getSelectedFile().toString());
				props.setProperty("txt.prefix", txtPrefix.getText());
				props.setProperty("txt.suffix", txtSuffix.getText());
				props.setProperty("txt.charset", txtCharset.getText());
				props.setProperty("txt.inFilename.suffix", txtInSuffix.getText());
				props.setProperty("txt.configFilename.suffix", txtConfigSuffix.getText());

				try (FileWriter fw = new FileWriter("saveLv0.properties");) {
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
				props.setProperty("currently.no.condition", "[WARNING01] currently.no.condition");
				
				try (FileWriter fw = new FileWriter("saveMsgLv0.properties");) {
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
		try (FileReader fr = new FileReader("./saveLv0.properties");) {
			Properties props = new Properties();
			props.load(fr);
			String inDir = props.getProperty("dir.in");
			if (inDir != null) {
				chooserIn.setSelectedFile(new File(inDir));
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
				+ "================= Level 0 ================= \n"
				+ "It's check the data format and character" 
				+ "==========================V1.0.20150406============= \n" );
	}

	/**
	 * Display the Path of Folder that be choose in the View.
	 */
	private void printFolderPaths() {
		ta.append("\n");
		ta.append("Data Input Directory: " + chooserIn.getSelectedFile().getAbsolutePath());
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

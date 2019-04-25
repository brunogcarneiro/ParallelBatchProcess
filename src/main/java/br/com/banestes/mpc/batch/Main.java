package br.com.banestes.mpc.batch;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import br.com.banestes.mpc.batch.parse.GenericParser;
import br.com.banestes.mpc.batch.parse.Parser;
import br.com.banestes.mpc.batch.read.Reader;
import br.com.banestes.mpc.batch.read.TextReader;
import br.com.banestes.mpc.batch.read.xml.LayoutTO;
import br.com.banestes.mpc.batch.read.xml.XMLLoader;
import br.com.banestes.mpc.batch.writer.OracleWriter;
import br.com.banestes.mpc.batch.writer.Writer;

public class Main {
	private static final Logger LOGGER = LogManager.getLogger(Main.class.getSimpleName());
	
	//INTERNAL PARAMS
	private static final String LAYOUT_FILE = "layout2.xml";
	
	/**
	 * EXTERNAL PARAMS
	 * 
	 * They will be passed through VM arguments when the process was executed
	 */
	private static final String INPUT_DIRECTORY = "inputDirectory";
	private static final String SUCCESS_FOLDER = "successFolder";
	private static final String FAILURE_FOLDER = "failureFolder";
	private static final String DS_NAME = "dsName";
	
	static LayoutTO layout;
	static ParallelBatchProcess myBatch;
	static Path sourcePath;
	static Reader myReader;
	static Parser myParser;
	static Writer myWriter;
	
	public static void main(String[] args) {	
		boolean processExecutedWithoutError = false;
		
		try {
			
			readLayout();
				
			initializeReader();
			initializeParser();
			initializeWriter();
			   
			//Threads need to reference each other in order they can comunicate
			setThreadReferences();
			
			processExecutedWithoutError = executeParallelBatchProcess();
			
		} catch(Exception e) {
			
			LOGGER.error(e);
			e.printStackTrace();
			
		} finally {
			
			//moveFileToDestinationFolder(processExecutedWithoutError);
			
		}
		
		System.exit(0);
	}
	
	private static void moveFileToDestinationFolder(boolean processExecutedWithoutError) {
		String destinationFolder = System.getProperty( processExecutedWithoutError ? SUCCESS_FOLDER : FAILURE_FOLDER);
		Path destinationPath = Paths.get(destinationFolder,sourcePath.getFileName().toString());
		
		try {
			Files.move(sourcePath, destinationPath);
		} catch (IOException e) {
			LOGGER.error(e);
		}
	}

	private static boolean executeParallelBatchProcess() {
		myBatch = new ParallelBatchProcess(myReader, myParser, myWriter);
		myBatch.setLogger(LOGGER);
		return myBatch.start();
	}

	private static void setThreadReferences() {
		myReader.setParser(myParser);
        
        myParser.setReader(myReader);
        myParser.setWriter(myWriter);
        
        myWriter.setParser(myParser);
        myWriter.setLogger(LOGGER);
	}

	private static void initializeWriter() throws ClassNotFoundException, SQLException {
		String dsname = System.getProperty(DS_NAME);
		
		if(StringUtils.isEmpty(dsname)) {
			myWriter = new OracleWriter("jdbc:oracle:thin:@10.8.8.40:1521:HML02","pmpce","pmpce001",layout);
		} else {
			myWriter = new OracleWriter(dsname,layout);
		}
	}

	private static void initializeParser() {
		myParser = new GenericParser(layout.getRecord("0"));
	}

	public static void readLayout() throws Exception {
		layout = XMLLoader.loadLayoutFromXML(LAYOUT_FILE);
	}
	
	public static void initializeReader() throws IOException {
		sourcePath = Paths.get(System.getProperty(INPUT_DIRECTORY),"example1.txt");
        myReader = new TextReader(sourcePath, layout.getRecord("0"));
	}
}

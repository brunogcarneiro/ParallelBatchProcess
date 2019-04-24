package br.com.spassu.parallelbatchprocess.writer;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import br.com.spassu.parallelbatchprocess.ProcessState;
import br.com.spassu.parallelbatchprocess.parse.Parser;
import br.com.spassu.parallelbatchprocess.read.xml.FieldTO;
import br.com.spassu.parallelbatchprocess.read.xml.LayoutTO;

public class OracleWriter implements Writer {
	private static final String PK_SEQUENCE_NAME = "pc_temp_sequence";
	private static final String PK_COLUMN_NAME = "ident";
	private static final int NOT_ZERO_BASED = 1; //The field's index in the list of values starts at 1, not 0.
	private static final int MAX_LIMIT_DB_BATCH = 1000; //The field's index in the list of values starts at 1, not 0.
	private static final int MIN_LIMIT_DB_BATCH = 1000;
	
	private int DELAY = 0;
	
	Connection conn;
	PreparedStatement stmt;
	List<String> orderedFields;
	LayoutTO layout;

	private ProcessState writerState = ProcessState.NOT_STARTED;
	private Parser parser;
	
	private boolean log = false;
	
	private ConcurrentLinkedQueue<Map<FieldTO,Object>> parsedRecordsQueue = new ConcurrentLinkedQueue<>();
	 
	StringBuilder mySql = new StringBuilder();
	
	
	public OracleWriter(String connectionString, String user, String pass, LayoutTO layout) throws SQLException, ClassNotFoundException{
		Class.forName ("oracle.jdbc.OracleDriver");
		conn = DriverManager.getConnection(connectionString,user,pass);
		this.layout = layout;

		orderedFields = 
		layout.getRecords().get(0)
			  .getFields().stream()
		      .map(FieldTO::getName)
		      .sorted()
		      .collect(Collectors.toList());
		
		List<String> columnList = new LinkedList<>();
		columnList.add(PK_COLUMN_NAME);
		columnList.addAll(orderedFields);
		
		mySql.setLength(0);
		
		mySql.append("INSERT INTO pc_temp");

		mySql.append(getColumnList(columnList));
		mySql.append(" VALUES (");
		mySql.append(getValuesList(orderedFields));	
		mySql.append(")");
		
		System.out.println(mySql);
		stmt = conn.prepareStatement(mySql.toString());	
	}
	
	@Override
	public void setParser(Parser parser) {
		this.parser = parser;
	}
	
	public boolean write(List<Map<FieldTO, Object>> parsedRecords) throws SQLException {
		
		parsedRecords
			.stream()
			.forEach(this::addRecordToStatement);
	
		delay();//Used to simulate communication overhead. Default is 0 ms;
		return stmt.executeBatch() != null;
	}
	
	private void addRecordToStatement(Map<FieldTO, Object> record) {
		
		record
			.entrySet()
			.stream()
			.forEach(this::addFieldToStatement);
		
		try {
			stmt.addBatch();
		} catch (SQLException e) {
			throw new RuntimeException("Erro ao tentar adicionar batch", e);
		}
	}
	
	private void addFieldToStatement(Entry<FieldTO, Object> field){
		FieldTO fieldTO = field.getKey();
		int columnIndex = orderedFields.indexOf(fieldTO.getName()) + NOT_ZERO_BASED;

		try {
			switch(fieldTO.getType()) {
				case "A":
					stmt.setString(columnIndex, (String) field.getValue());
					break;
				case "N":
					stmt.setBigDecimal(columnIndex, (BigDecimal) field.getValue());
					break;
				default:
					throw new RuntimeException("Tipo de campo não reconhecido.");
			}
		}catch (SQLException e) {
			throw new RuntimeException("Erro de SQL ao adicionar field ao statement.", e);
		}
	}

	private void delay() {
		if(DELAY < 1) return;
		
		try {
			Thread.sleep(DELAY);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private String getValuesList(List<String> orderedFields) {
		Stream<String> nextVal = 
			Arrays.asList(PK_SEQUENCE_NAME+".nextval")
				  .stream();
		
		Stream<String> questionMarks =
			orderedFields.stream()
						 .map(field -> "?");
		
		List<String> values = 
			Stream.concat(nextVal, questionMarks)
				  .collect(Collectors.toList());
		
		return String.join(", ", values);
	}

	private String getColumnList(List<String> orderedFields) {
		StringBuilder columnList = new StringBuilder();

		columnList.append("(");
		columnList.append(String.join(", ", orderedFields));
		columnList.append(")");
		
		return columnList.toString();
	}

	@Override
	public void close() {
		try {
			stmt.close();
			conn.close();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void cleanTable() throws SQLException {
		conn.createStatement().execute("TRUNCATE TABLE pc_temp");
	}

	@Override
	public void setDelay(int miliseconds) {
		DELAY = miliseconds;
	}

	@Override
	public void pushParsedItem(Object item) {
		Map<FieldTO, Object> parsed = (Map<FieldTO, Object>) item;
		
		logParsedString(parsed);
		parsedRecordsQueue.add(parsed);
	}
	
	private Map<FieldTO, Object> logParsedString(Map<FieldTO,Object> record){
		if (log) System.out.println("PARSED: "+ record.toString());
		return record;
	}

	private void notifyWriterState(ProcessState newState) {
		writerState = newState;
		System.out.println("Writer is " + newState);
	}

	@Override
	public ProcessState getState() {
		return this.writerState;
	}

	@Override
	public int countParsedItemReadyToWrite() {
		return parsedRecordsQueue.size();
	}
	
	private long getBatchLimit(int queueSize) {
		int batchLimit = (MAX_LIMIT_DB_BATCH > 0) ? MAX_LIMIT_DB_BATCH : Integer.MAX_VALUE;
		return (batchLimit < queueSize) 
					? batchLimit 
					: queueSize;
	}
	
	private void waitSeconds(int seconds) {
		try {
			Thread.sleep(seconds*1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private boolean existsItemsToWrite() {
		return parser.getState() != ProcessState.DONE || !parsedRecordsQueue.isEmpty();
	}

	@Override
	public Boolean call() throws Exception {

		int count = 0;
		
		List<Map<FieldTO, Object>> parsedRecordsList;
		
		notifyWriterState(ProcessState.RUNNING);
		
		while (existsItemsToWrite()) {

			/*if (count++ > 500) {
				throw new Exception("Test exception in the Writer");
			}*/

			if (hasEnoughParsedItemsToWrite()) {

				parsedRecordsList = 
				Stream.iterate(0, n -> n++)
					  .limit(getBatchLimit(parsedRecordsQueue.size()))
					  .map(n -> parsedRecordsQueue.poll())
					  .collect(Collectors.toList());

				System.out.println("Write: "+parsedRecordsList.size());
				write(parsedRecordsList);
				
				parsedRecordsList = null;
			} else {
				waitSeconds(1);
			}
		}
		
		this.close();

		notifyWriterState(ProcessState.DONE);

		return true;
	}

	private boolean hasEnoughParsedItemsToWrite() {
		boolean reachMinBatchLimit = parsedRecordsQueue.size() >= MIN_LIMIT_DB_BATCH;
		boolean parseIsDone = parser.getState() == ProcessState.DONE;
		
		return reachMinBatchLimit || parseIsDone;
	}
}

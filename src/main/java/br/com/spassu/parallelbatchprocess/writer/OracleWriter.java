package br.com.spassu.parallelbatchprocess.writer;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import br.com.spassu.parallelbatchprocess.ProcessState;
import br.com.spassu.parallelbatchprocess.parse.Parser;
import br.com.spassu.parallelbatchprocess.read.xml.FieldTO;
import br.com.spassu.parallelbatchprocess.read.xml.LayoutTO;
import br.com.spassu.parallelbatchprocess.read.xml.RecordTO;

public class OracleWriter implements Writer {
	private static final String PK_SEQUENCE_NAME = "pc_temp_sequence";
	private static final String PK_COLUMN_NAME = "ident";
	private static final int NOT_ZERO_BASED = 1; //The field's index in the list of values starts at 1, not 0.
	
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
		layout.getRecords().get(0).getFields()
		  	  .stream()
		      .map(FieldTO::getName)
		      .sorted()
		      .collect(Collectors.toList());
		
		List<String> columnList = 
				Stream.of(PK_COLUMN_NAME)
					  .collect(Collectors.toList());
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
			e.printStackTrace();
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
	}

	@Override
	public ProcessState getState() {
		return this.writerState;
	}

	@Override
	public int countParsedItemReadyToWrite() {
		return parsedRecordsQueue.size();
	}

	@Override
	public Boolean call() throws Exception {
		boolean result;
		try {
			notifyWriterState(ProcessState.RUNNING);
			
			while (parser.getState() != ProcessState.DONE || !parsedRecordsQueue.isEmpty()) {
				
				if (parser.getState() == ProcessState.ERROR) {
					throw new Exception("Writer interrompido por falha no Parser.");
				}
				
				if (!parsedRecordsQueue.isEmpty()) {
					List<Map<FieldTO, Object>> parsedRecordsList = new LinkedList<>();
					
					while(!parsedRecordsQueue.isEmpty()) {
						parsedRecordsList.add(parsedRecordsQueue.poll());
					}
							
					try {
						//printMonitor();
						System.out.println("WRITE: " + parsedRecordsList.size());
						write(parsedRecordsList);
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					parsedRecordsList = null;
					Runtime.getRuntime().gc();
				}
			}
			
			this.close();

			notifyWriterState(ProcessState.DONE);
			result = true;
			
		} catch (Throwable t) {
			System.out.println("Falha no Writer: " + t.getMessage());
			notifyWriterState(ProcessState.ERROR);
			result = false;
		}
		
		return result;
	}
}

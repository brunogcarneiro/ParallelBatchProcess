package br.com.spassu.parallelbatchprocess.writer;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import br.com.spassu.parallelbatchprocess.read.xml.FieldTO;
import br.com.spassu.parallelbatchprocess.read.xml.LayoutTO;
import br.com.spassu.parallelbatchprocess.read.xml.RecordTO;

public class OracleWriter implements Writer {
	private static final String PK_SEQUENCE_NAME = "pc_temp_sequence";
	private static final String PK_COLUMN_NAME = "ident";
	private static final int NOT_ZERO_BASED = 1; //The field's index in the list of values starts at 1, not 0.
	private static final int SKIP_SEQUENCE_NEXTAVAL = 0; //The first place in the list of values belongs to the PK_SEQUENCE_NAME.nextval
	
	private int DELAY = 0;
	
	Connection conn;
	PreparedStatement stmt;
	List<String> orderedFields;
	LayoutTO layout;
	 
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
					throw new RuntimeException("Tipo de campo n�o reconhecido.");
			}
		}catch (SQLException e) {
			throw new RuntimeException("Erro de SQL ao adicionar field ao statement.", e);
		}
	}
	
	private FieldTO getFieldTO(int key) {
		Optional<FieldTO> fieldTO =layout
			.getRecords().get(0)
			.getFields()
			.stream()
			.filter(f -> (f.getStart() == key))
			.findFirst();
		
		return fieldTO.get();
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
	
	private String getRecordValues(Map<String, Object> record) {
		StringBuilder recordValues = new StringBuilder();
		recordValues.append("(");
		recordValues.append(separateMapValuesByComma(record));
		recordValues.append(")");
		
		return recordValues.toString();
	}

	private String getColumnList(List<String> orderedFields) {
		StringBuilder columnList = new StringBuilder();

		columnList.append("(");
		columnList.append(String.join(", ", orderedFields));
		columnList.append(")");
		
		return columnList.toString();
	}
	
	private String separateMapValuesByComma(Map<String, Object> map) {
		List<String> values = orderedFields
			.stream()
			.map(map::get)
			.map(Object::toString)
			.map(this::applyQuotes)
			.collect(Collectors.toList());
		
		return String.join(", ", values);
	}
	
	private String applyQuotes(String str) {
		StringBuilder sb = new StringBuilder();
		sb.append("'");
		sb.append(str);
		sb.append("'");
		return sb.toString();
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
		stmt.execute("DELETE FROM pc_temp");
	}

	@Override
	public void setDelay(int miliseconds) {
		DELAY = miliseconds;
	}
}

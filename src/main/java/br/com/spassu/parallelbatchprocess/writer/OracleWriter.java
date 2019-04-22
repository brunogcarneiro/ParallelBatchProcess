package br.com.spassu.parallelbatchprocess.writer;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import br.com.spassu.parallelbatchprocess.read.xml.FieldTO;
import br.com.spassu.parallelbatchprocess.read.xml.LayoutTO;
import br.com.spassu.parallelbatchprocess.read.xml.RecordTO;

public class OracleWriter implements Writer {
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
		
		mySql.setLength(0);
		
		mySql.append("INSERT INTO pc_temp");

		mySql.append(getColumnList(orderedFields));
		mySql.append(" VALUES ");
		mySql.append(getValuesList(orderedFields));	
		mySql.append(";");
		
		stmt = conn.prepareStatement(mySql.toString());
	}
	
	public boolean write(List<Map<FieldTO, Object>> parsedRecords) throws SQLException {
		
		parsedRecords
			.stream()
			.forEach(this::addRecordToStatement);
	
		delay();//Used to simulate communication overhead. Default is 0 ms;
		return stmt.execute();
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
		int columnIndex = orderedFields.indexOf(fieldTO.getName()) + 1;
		
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
		List<String> values =
			orderedFields
				.stream()
				.map(str -> "?")
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

package br.com.spassu.parallelbatchprocess.writer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class H2Writer implements Writer {
	private int DELAY = 0;
	
	Connection conn;
	Statement stmt;
	List<String> orderedFields;
	
	private final String INSERT = "INSERT INTO pc_temp"; 
	StringBuilder mySql = new StringBuilder();
	
	
	public H2Writer(String connectionString, String user, String pass) throws SQLException, ClassNotFoundException{
		Class.forName ("oracle.jdbc.OracleDriver");
		conn = DriverManager.getConnection(connectionString,user,pass);
		stmt  = conn.createStatement();
	}
	
	public boolean write(List<Map<String, Object>> parsedRecords) throws SQLException {
		orderedFields = parsedRecords.get(0)
				.entrySet()
				.stream()
				.map(Entry::getKey)
				.sorted()
				.collect(Collectors.toList());
		
		mySql.setLength(0);
		
		mySql.append(INSERT);
		mySql.append(" ");
		mySql.append(getColumnList(orderedFields));
		mySql.append(" VALUES ");
		mySql.append(getValuesList(parsedRecords));	
		mySql.append(";");
		
		delay();//Used to simulate communication overhead. Default is 0 ms;
		return stmt.execute(mySql.toString());
	}
	
	private void delay() {
		if(DELAY < 1) return;
		
		try {
			Thread.sleep(DELAY);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private String getValuesList(List<Map<String, Object>> parsedRecords) {
		List<String> values =
			parsedRecords
				.stream()
				.map(this::getRecordValues)
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

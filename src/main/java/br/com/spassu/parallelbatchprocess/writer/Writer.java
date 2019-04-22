package br.com.spassu.parallelbatchprocess.writer;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import br.com.spassu.parallelbatchprocess.read.xml.FieldTO;

public interface Writer {
	boolean write(List<Map<FieldTO, Object>> parsedRecordsList) throws SQLException;
	void close();
	void cleanTable() throws SQLException;
	
	/**
	 * The writer will wait for miliseconds between write lines. It is usefull to simulate communication overhead.
	 * @param miliseconds
	 */
	void setDelay(int miliseconds);
}

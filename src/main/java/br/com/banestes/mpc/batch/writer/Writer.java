package br.com.banestes.mpc.batch.writer;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.logging.log4j.Logger;

import br.com.banestes.mpc.batch.ProcessState;
import br.com.banestes.mpc.batch.parse.Parser;
import br.com.banestes.mpc.batch.read.xml.FieldTO;

public interface Writer extends Callable<Boolean>{
	boolean write(List<Map<FieldTO, Object>> parsedRecordsList) throws SQLException;
	void close();
	void cleanTable() throws SQLException;
	
	/**
	 * The writer will wait for miliseconds between write lines. It is usefull to simulate communication overhead.
	 * @param miliseconds
	 */
	void setDelay(int miliseconds);
	
	ProcessState getState();
	
	void pushParsedItem(Object item);
	
	int countParsedItemReadyToWrite();
	
	void setParser(Parser parser);
	
	void setLogger(Logger logger);
}

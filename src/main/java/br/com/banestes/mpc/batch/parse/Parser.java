package br.com.banestes.mpc.batch.parse;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

import br.com.banestes.mpc.batch.ProcessState;
import br.com.banestes.mpc.batch.read.Reader;
import br.com.banestes.mpc.batch.read.xml.FieldTO;
import br.com.banestes.mpc.batch.writer.Writer;

public interface Parser extends Callable<Boolean> {
	CompletableFuture<Map<FieldTO, Object>> parse(Object recordStr);
	
	/**
	 * The parser will wait for miliseconds between parse lines. It is usefull to simulate communication overhead.
	 * @param miliseconds
	 */
	void setDelay(int miliseconds);
	
	void pushReadItem(Object parsedItem);
	
	int countReadItemsReadyToParse();
	
	ProcessState getState();
	
	void setReader(Reader reader);
	
	void setWriter(Writer writer);
}

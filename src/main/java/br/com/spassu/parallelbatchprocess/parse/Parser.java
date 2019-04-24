package br.com.spassu.parallelbatchprocess.parse;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

import br.com.spassu.parallelbatchprocess.ProcessState;
import br.com.spassu.parallelbatchprocess.read.Reader;
import br.com.spassu.parallelbatchprocess.read.xml.FieldTO;
import br.com.spassu.parallelbatchprocess.writer.Writer;

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

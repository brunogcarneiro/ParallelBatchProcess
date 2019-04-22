package br.com.spassu.parallelbatchprocess.parse;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import br.com.spassu.parallelbatchprocess.read.xml.FieldTO;
import br.com.spassu.parallelbatchprocess.read.xml.LayoutTO;
import br.com.spassu.parallelbatchprocess.read.xml.RecordTO;

public interface Parser {
	CompletableFuture<Map<FieldTO, Object>> parse(String recordStr, RecordTO layout);
	
	/**
	 * The parser will wait for miliseconds between parse lines. It is usefull to simulate communication overhead.
	 * @param miliseconds
	 */
	void setDelay(int miliseconds);
}

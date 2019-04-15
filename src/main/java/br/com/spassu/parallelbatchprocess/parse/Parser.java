package br.com.spassu.parallelbatchprocess.parse;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import br.com.spassu.parallelbatchprocess.read.xml.LayoutTO;
import br.com.spassu.parallelbatchprocess.read.xml.RecordTO;

public interface Parser {
	CompletableFuture<Map<String, Object>> parse(Map<String, String> recordStr, RecordTO layout);
}

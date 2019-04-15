package br.com.spassu.parallelbatchprocess.read;

import java.util.Map;
import java.util.stream.Stream;

/**
 * An object responsible for read a file and expose its content as an Stream of Maps.
 * @author bruno.carneiro
 *
 */
public interface Reader {
	/**
	 * Each Map represents a Record in the read file. The keys will be the Field names and the value, the string representation as it is in the read file.
	 * @return A Stream of Maps that represents the Records in the read file.
	 */
	Stream<Map<String, String>> getRecordsMap();
	
	//ReaderState getState();
}
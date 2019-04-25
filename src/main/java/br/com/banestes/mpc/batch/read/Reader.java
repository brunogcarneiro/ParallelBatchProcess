package br.com.banestes.mpc.batch.read;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

import br.com.banestes.mpc.batch.ProcessState;
import br.com.banestes.mpc.batch.parse.Parser;

/**
 * An object responsible for read a file and expose its content as an Stream of Maps.
 * @author bruno.carneiro
 *
 */
public interface Reader extends Callable<Boolean> {
	/**
	 * Each Map represents a Record in the read file. The keys will be the Field names and the value, the string representation as it is in the read file.
	 * @return A Stream of Maps that represents the Records in the read file.
	 */
	Stream<String> getRecordsMap();
	
	/**
	 * The reader will wait for miliseconds between read lines. It is usefull to simulate communication overhead.
	 * @param miliseconds
	 */
	void setDelay(int miliseconds);
	
	public Stream<String> lines();
	
	ProcessState getState();
	
	public void setParser(Parser parser);
}
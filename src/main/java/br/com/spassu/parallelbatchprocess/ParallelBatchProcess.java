package br.com.spassu.parallelbatchprocess;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import br.com.spassu.parallelbatchprocess.parse.Parser;
import br.com.spassu.parallelbatchprocess.read.Reader;
import br.com.spassu.parallelbatchprocess.read.xml.RecordTO;
import br.com.spassu.parallelbatchprocess.writer.Writer;

public class ParallelBatchProcess {
	private Reader reader;
	private Parser parser;
	private Writer writer;
	private Future<Boolean> writerFuture;
	

	
	public ParallelBatchProcess(Reader reader, Parser parser, RecordTO recordLayout, Writer writer) throws IOException {
		this.reader = reader;
		this.parser = parser;
		this.writer = writer;
	}

	public boolean start() {
		boolean result;
		
		try {
			Future<Boolean> processTermination = startAllThreads();
			result = processTermination.get();
			//waitSeconds(30);
			printMonitor();
			System.out.println("Finished!");
		} catch (Throwable t) {
			result = false;
			System.out.println("Terminou com erro");
		}
		
		System.out.println(result);
		return result;
	}
	
	private void awaitProcessTermination(List<Future<Boolean>> allThreadsFuture) throws InterruptedException, ExecutionException {
		writerFuture.get();
		System.out.println(writer.getState());
	}

	private Future<Boolean> startAllThreads() {
		ExecutorService service = Executors.newCachedThreadPool();
		
		try {
			service.submit(reader);
			service.submit(parser);
			return service.submit(writer);
		} finally {
			if(service != null) service.shutdown();
		}
	}
	
	private void waitSeconds(int seconds) {
		try {
			Thread.sleep(30*1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void printMonitor() {
		System.out.println(
				   "readSize: "    + parser.countReadItemsReadyToParse() +
				" / parsedSize: "  + writer.countParsedItemReadyToWrite() +
				" / ReaderState: " + reader.getState() +
				" / ParserState: " + parser.getState() +
				" / WriterState: " + writer.getState()
			);
	}
}
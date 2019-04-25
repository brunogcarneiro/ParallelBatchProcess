package br.com.banestes.mpc.batch;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;

import br.com.banestes.mpc.batch.parse.Parser;
import br.com.banestes.mpc.batch.read.Reader;
import br.com.banestes.mpc.batch.writer.Writer;

public class ParallelBatchProcess {	
	private Reader reader;
	private Parser parser;
	private Writer writer;
	private Future<Boolean> writerFuture;
	CompletionService<Boolean> service;
	
	private static Logger LOGGER;
	
	public ParallelBatchProcess(Reader reader, Parser parser, Writer writer) {
		this.reader = reader;
		this.parser = parser;
		this.writer = writer;
	}

	public boolean start() {
		boolean result;
		
		try {
			List<Future<Boolean>> futureList = startAllThreads();
			//startMonitor();
			result = awaitProcessTermination(futureList);
			//waitSeconds(30);
			LOGGER.debug("Finished!");
		} catch (Throwable t) {
			result = false;
			LOGGER.debug("Terminou com erro");
		}
		
		LOGGER.debug(result);
		return result;
	}
	
	private Boolean awaitProcessTermination(List<Future<Boolean>> remainingFuturesList) throws InterruptedException, ExecutionException {
		Future<Boolean> completedFuture = null;
		Boolean result = true;
		
		while (!remainingFuturesList.isEmpty()) {
		    
			// block until a thread completes
		    completedFuture = service.take();
		    remainingFuturesList.remove(completedFuture);
		 
		    // get the result, if the Callable was able to create it
		    try {
		    	result = result && completedFuture.get();
		    } catch (ExecutionException e) {
		        Throwable cause = e.getCause();
		        LOGGER.debug("Falha no processo batch: " + cause);
		        cause.printStackTrace();
		        
		        //Stop remaining threads
		        remainingFuturesList
		        	.stream()
		        	.forEach(f -> f.cancel(true));
		 
		        result = false;
		        break;
		    }
		}
		
		return result;
	}

	private List<Future<Boolean>> startAllThreads() throws InterruptedException {
		service = new ExecutorCompletionService<Boolean>(Executors.newCachedThreadPool());
		
		List<Future<Boolean>> remainingFuturesList = new LinkedList<>();
		remainingFuturesList.add(service.submit(reader));
		remainingFuturesList.add(service.submit(parser));
		remainingFuturesList.add(service.submit(writer));
		
		return remainingFuturesList;
	}
		
	
	private void waitSeconds(int seconds) {
		try {
			Thread.sleep(seconds*1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void startMonitor() {
		ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
		exec.scheduleAtFixedRate(() -> {
			LOGGER.debug(
					   "readSize: "    + parser.countReadItemsReadyToParse() +
					" / parsedSize: "  + writer.countParsedItemReadyToWrite() +
					" / ReaderState: " + reader.getState() +
					" / ParserState: " + parser.getState() +
					" / WriterState: " + writer.getState()
				);
		}, 0, 5, TimeUnit.SECONDS);
	}
	
	public void setLogger(Logger logger) {
		LOGGER = logger;
	}
}
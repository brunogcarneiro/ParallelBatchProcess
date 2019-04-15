package br.com.spassu.parallelbatchprocess;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import javax.swing.plaf.SliderUI;

import br.com.spassu.parallelbatchprocess.parse.Field;
import br.com.spassu.parallelbatchprocess.parse.Parser;
import br.com.spassu.parallelbatchprocess.read.Reader;
import br.com.spassu.parallelbatchprocess.read.TextReader;
import br.com.spassu.parallelbatchprocess.read.xml.FieldTO;
import br.com.spassu.parallelbatchprocess.read.xml.LayoutTO;
import br.com.spassu.parallelbatchprocess.read.xml.RecordTO;

public class ParallelBatchProcess {
	private RecordTO recordLayout;
	private Reader reader;
	private Parser parser;
	private ProcessState readerState = ProcessState.NOT_STARTED;
	private ProcessState parserState = ProcessState.NOT_STARTED;
	
	private ConcurrentLinkedQueue<Map<String,String>> readRecordsQueue = new ConcurrentLinkedQueue<>();
	private ConcurrentLinkedQueue<Map<String,Object>> parsedRecordsQueue = new ConcurrentLinkedQueue<>();
	//private ConcurrentLinkedQueue<Record> parsedRecords; 
	
	public ParallelBatchProcess(Reader reader, Parser parser, RecordTO recordLayout) throws IOException {

		this.reader = reader;
		this.parser = parser;
		this.recordLayout = recordLayout;
	}

	public void start() {
		ExecutorService service = null;
		try {
			service = Executors.newCachedThreadPool();
			
			service.execute(this::readRecords);
			service.execute(this::parseRecords);
	
		} finally {
			if(service != null) service.shutdown();
		}
		
		//new Thread(this::readRecords).start();
		
		while(parserState != ProcessState.DONE) {
			waitAleatory();
		}
		
		System.out.println("Finished!");
	}
	
	public void printRecords() {
		reader
		 .getRecordsMap()
		 .limit(5)
		 .forEach(System.out::println);
	}
	
	private void printLine(String str) {
		System.out.println("["+str+"]");
	}
	
	private void readRecords() {
		reader
		 .getRecordsMap()
		 .limit(5)
		 .map(this::logReadString)
		 .map(readRecordsQueue::add)
		 .forEach(this::setReaderStateToRunning);
		
		readerState = ProcessState.DONE;
	}
	
	private String extractCpf(Map<String,String> record) {
		return record.get("CPF_CNPJ").toString();
	}
	
	private Map<String, Object> logParsedString(Map<String,Object> record){
		System.out.println("PARSED: "+ record.toString());
		return record;
	}
	
	private Map<String, String> logReadString(Map<String,String> record){
		System.out.println("READ: "+ extractCpf(record));
		return record;
	}
	
	private void parseRecords(){
		
		List<Future<Void>> futureList = new LinkedList<>();
		
		parserState = ProcessState.RUNNING;
		
		while (!readRecordsQueue.isEmpty() || readerState != ProcessState.DONE) {
			if (!readRecordsQueue.isEmpty()) {
				try {
					futureList.add(
						parser
							.parse(readRecordsQueue.poll(), recordLayout)
							.thenAccept(this::addParsed)
					);
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				
			}
		}
		
		CompletableFuture<Void> allFutures = CompletableFuture.allOf(futureList.toArray(new CompletableFuture[futureList.size()]));
		try {
			allFutures.get();
		} catch (InterruptedException | ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		parserState = ProcessState.DONE;
	}
	
	private void addParsed(Map<String, Object> parsed){
		parsedRecordsQueue.add(parsed);
		logParsedString(parsed);
	}
	
	private void setReaderStateToRunning(boolean isRunning) {
		readerState = ProcessState.RUNNING;
		 waitAleatory();
	}
	
	private void waitAleatory() {
		try {
			Thread.sleep(new Random().nextInt(200));
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
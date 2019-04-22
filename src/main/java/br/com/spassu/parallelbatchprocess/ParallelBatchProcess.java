package br.com.spassu.parallelbatchprocess;

import java.io.IOException;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import br.com.spassu.parallelbatchprocess.parse.Parser;
import br.com.spassu.parallelbatchprocess.read.ReadLineHelper;
import br.com.spassu.parallelbatchprocess.read.Reader;
import br.com.spassu.parallelbatchprocess.read.xml.FieldTO;
import br.com.spassu.parallelbatchprocess.read.xml.RecordTO;
import br.com.spassu.parallelbatchprocess.writer.Writer;

public class ParallelBatchProcess {
	private RecordTO recordLayout;
	private Reader reader;
	private Parser parser;
	private Writer writer;
	private ProcessState readerState = ProcessState.NOT_STARTED;
	private ProcessState parserState = ProcessState.NOT_STARTED;
	private ProcessState writerState = ProcessState.NOT_STARTED;
	private boolean log = true;
	private boolean logProcessState = false;
	
	private ConcurrentLinkedQueue<String> readRecordsQueue = new ConcurrentLinkedQueue<>();
	private ConcurrentLinkedQueue<Map<FieldTO,Object>> parsedRecordsQueue = new ConcurrentLinkedQueue<>();
	//private ConcurrentLinkedQueue<Record> parsedRecords; 
	
	public ParallelBatchProcess(Reader reader, Parser parser, RecordTO recordLayout, Writer writer) throws IOException {
		this.reader = reader;
		this.parser = parser;
		this.writer = writer;
		this.recordLayout = recordLayout;
	}

	public void start() {
		ExecutorService service = null;
		try {
			service = Executors.newCachedThreadPool();
			
			service.execute(this::readRecords);
			//service.execute(this::parseRecords);
			//service.execute(this::writeRecords);
	
		} finally {
			if(service != null) service.shutdown();
		}
		
		//while (writerState != ProcessState.DONE) {
		while (readerState != ProcessState.DONE) {
			try {
				Runtime.getRuntime().gc();
				Thread.sleep(30*1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		printMonitor();
		System.out.println("Finished!");
	}
	
	private void printMonitor() {
		System.out.println(
				   "readSize: "    + readRecordsQueue.size() +
				" / parsedSize: "  + parsedRecordsQueue.size() +
				" / ReaderState: " + readerState +
				" / ParserState: " + parserState +
				" / WriterState: " + writerState
			);
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
		 //.lines()
		 //.limit(10000)
		 //.map(this::logReadString)
		 .map(readRecordsQueue::add)
		 .forEach(this::setReaderStateToRunning);
		
		Runtime.getRuntime().gc();
		setReaderState(ProcessState.DONE);
	}
	
	private void setReaderState(ProcessState newState) {
		if (logProcessState) System.out.println("ReaderState: "+newState);
		readerState = newState;
	}
	
	private void setWriterState(ProcessState newState) {
		if (logProcessState) System.out.println("WriterState: "+newState);
		writerState = newState;
	}
	
	private void setParserState(ProcessState newState) {
		if (logProcessState) System.out.println("ParserState: "+newState);
		parserState = newState;
	}
	
	private String extractCpf(String record) {
		FieldTO cpf = ReadLineHelper.getFieldFromLayout("CPF_CNPJ",recordLayout);
		return ReadLineHelper.read(record, cpf);
	}
	
	private Map<FieldTO, Object> logParsedString(Map<FieldTO,Object> record){
		if (log) System.out.println("PARSED: "+ record.toString());
		return record;
	}
	
	private String logReadString(String record){
		if (log) System.out.println("READ: "+ extractCpf(record));
		return record;
	}
	
	private void parseRecords(){
		
		List<Future<Void>> futureList = new LinkedList<>();
		
		setParserState(ProcessState.RUNNING);
		
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
		
		Runtime.getRuntime().gc();
		setParserState(ProcessState.DONE);
	}
	
	private void addParsed(Map<FieldTO, Object> parsed){
		logParsedString(parsed);
		parsedRecordsQueue.add(parsed);
	}
	
	private void setReaderStateToRunning(boolean isRunning) {
		setReaderState(ProcessState.RUNNING);
	}
	
	private void writeRecords(){	
		setWriterState(ProcessState.RUNNING);
		
		while (parserState != ProcessState.DONE || !parsedRecordsQueue.isEmpty()) {
			if (!parsedRecordsQueue.isEmpty()) {
				List<Map<FieldTO, Object>> parsedRecordsList = new LinkedList<>();
				
				while(!parsedRecordsQueue.isEmpty()) {
					parsedRecordsList.add(parsedRecordsQueue.poll());
				}
				
				Runtime.getRuntime().gc();
						
				try {
					printMonitor();
					System.out.println("WRITE: " + parsedRecordsList.size());
					writer.write(parsedRecordsList);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				parsedRecordsList = null;
				Runtime.getRuntime().gc();
			}
		}
		
		writer.close();
		
		Runtime.getRuntime().gc();
		setWriterState(ProcessState.DONE);
	}
}
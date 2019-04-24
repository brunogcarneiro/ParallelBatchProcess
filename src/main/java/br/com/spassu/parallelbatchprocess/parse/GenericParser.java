package br.com.spassu.parallelbatchprocess.parse;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import br.com.spassu.parallelbatchprocess.ProcessState;
import br.com.spassu.parallelbatchprocess.read.ReadLineHelper;
import br.com.spassu.parallelbatchprocess.read.Reader;
import br.com.spassu.parallelbatchprocess.read.xml.FieldTO;
import br.com.spassu.parallelbatchprocess.read.xml.RecordTO;
import br.com.spassu.parallelbatchprocess.writer.Writer;

public class GenericParser implements Parser{
	private int DELAY = 0;
	
	private ConcurrentLinkedQueue<String> readRecordsQueue = new ConcurrentLinkedQueue<>();
	private ProcessState parserState = ProcessState.NOT_STARTED;
	private Writer writer;
	private Reader reader;

	private RecordTO recordLayout; 
	
	
	
	public GenericParser(RecordTO recordLayout) {
		super();
		this.recordLayout = recordLayout;
	}
	
	public void setWriter(Writer writer) {
		this.writer = writer;
	}
	
	public void setReader(Reader reader) {
		this.reader = reader;
	}

	@Override
	public CompletableFuture<Map<FieldTO, Object>> parse(Object record) {
		
		String recordStr = (String) record;
		
		
		@SuppressWarnings("unchecked")
		CompletableFuture<Map<FieldTO, Object>> future = CompletableFuture.supplyAsync(new Supplier() {
		    @Override
		    public Map<FieldTO, Object> get() {

		        Map<FieldTO, Object> result = 
		        	recordLayout
					 .getFields()
					 .stream()
					 .collect(
						Collectors.toMap(Function.identity(), x -> extractValue(recordStr,x))
					 );
				
		        delay();//Used to simulate communication overhead. Default is 0 ms;
				return result;
		    }
		});
		
		
		return future;
	}
	
	private void delay() {
		if(DELAY < 1) return;
		
		try {
			Thread.sleep(DELAY);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private Object extractValue(String recordStr, FieldTO fieldTO) {
		String valueStr = ReadLineHelper.read(recordStr, fieldTO);
		Object value = Field.getInstance(valueStr, fieldTO.getType()).getValue();
		return value;
	}

	@Override
	public void setDelay(int miliseconds) {
		DELAY = miliseconds;
	}

	@Override
	public void pushReadItem(Object parsedItem) {
		readRecordsQueue.add((String) parsedItem);
	}

	@Override
	public int countReadItemsReadyToParse() {
		return readRecordsQueue.size();
	}

	@Override
	public ProcessState getState() {
		return parserState;
	}

	@Override
	public Boolean call() throws Exception {
		try {
			int count = 0;
			
			List<Future<Void>> futureList = new LinkedList<>();
			
			this.parserState = ProcessState.RUNNING;
			
			while (!readRecordsQueue.isEmpty() || reader.getState() != ProcessState.DONE) {
				
				if (count++ > 500) {
					throw new RuntimeException("Test erro no parser");
				}
				
				if (reader.getState() == ProcessState.ERROR) {
					throw new Exception("Parser interrompido por falha no Reader.");
				}
				
				if (!readRecordsQueue.isEmpty()) {
					try {
						futureList.add(
							parse(readRecordsQueue.poll())
								.thenAccept(writer::pushParsedItem)
						);
					} catch (Exception e1) {
						e1.printStackTrace();
					}
					
				}
			}
			
			CompletableFuture<Void> allFutures = CompletableFuture.allOf(futureList.toArray(new CompletableFuture[futureList.size()]));
		
			allFutures.get();
			
			this.parserState = ProcessState.DONE;
		
		} catch (Throwable t) {
			System.out.println("Falha no Parser: " + t.getMessage());
			this.parserState = ProcessState.ERROR;
			
		}
		
		return null;
	}
}

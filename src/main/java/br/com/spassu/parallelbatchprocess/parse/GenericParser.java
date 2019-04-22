package br.com.spassu.parallelbatchprocess.parse;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import br.com.spassu.parallelbatchprocess.read.ReadLineHelper;
import br.com.spassu.parallelbatchprocess.read.xml.FieldTO;
import br.com.spassu.parallelbatchprocess.read.xml.RecordTO;

public class GenericParser implements Parser{
	private int DELAY = 0;
	
	@Override
	public CompletableFuture<Map<FieldTO, Object>> parse(String recordStr, RecordTO layout) {
		
		
		@SuppressWarnings("unchecked")
		CompletableFuture<Map<FieldTO, Object>> future = CompletableFuture.supplyAsync(new Supplier() {
		    @Override
		    public Map<FieldTO, Object> get() {

		        Map<FieldTO, Object> result = 
		        	layout
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

}

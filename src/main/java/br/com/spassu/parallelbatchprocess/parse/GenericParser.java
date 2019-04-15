package br.com.spassu.parallelbatchprocess.parse;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import br.com.spassu.parallelbatchprocess.read.xml.FieldTO;
import br.com.spassu.parallelbatchprocess.read.xml.RecordTO;

public class GenericParser implements Parser{

	@Override
	public CompletableFuture<Map<String, Object>> parse(Map<String, String> recordStr, RecordTO layout) {
		
		
		@SuppressWarnings("unchecked")
		CompletableFuture<Map<String, Object>> future = CompletableFuture.supplyAsync(new Supplier() {
		    @Override
		    public Map<String, Object> get() {
		        try {
		            TimeUnit.SECONDS.sleep(new Random().nextInt(1)); 
		        } catch (InterruptedException e) {
		            throw new IllegalStateException(e);
		        }
		        
		        Map<String, Object> result = layout
											 .getFields()
											 .stream()
											 .collect(
												Collectors.toMap(FieldTO::getName, x -> extractValue(recordStr,x))
											 );
				
				return result;
		    }
		});
		
		return future;
	}
	
	private Object extractValue(Map<String, String> recordStr, FieldTO fieldTO) {
		String valueStr = recordStr.get(fieldTO.getName());
		Object value = Field.getInstance(valueStr, fieldTO.getType()).getValue();
		return value;
	}

}

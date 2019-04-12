package br.com.spassu.batchfile;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import br.com.spassu.batchfile.model.Field;
import br.com.spassu.batchfile.xml.FieldTO;
import br.com.spassu.batchfile.xml.RecordTO;

public class BatchFile {
	private RecordTO recordLayout;
	private RecordBufferedReader br;
	
	public BatchFile(RecordTO recordLayout, String filePath) throws IOException {
		this.recordLayout = recordLayout;
		
		this.br = new RecordBufferedReader(filePath, recordLayout);
	}

	
	public void printRecords() {
		br.recordStream()
		.limit(5)
		.map(this::createRecord)
		.forEach(System.out::println);
	}
	
	private void printLine(String str) {
		System.out.println("["+str+"]");
	}
	
	private Map<String, Object> createRecord(String recordStr) {
		Map<String, Object> record = new HashMap<>();
		
		recordLayout.getFields().stream()
			.forEach(
				fieldTO -> 
					record.put(
							fieldTO.getName(), 
							extractValue(recordStr, fieldTO)
					)
			);
		return record;
	}
	
	private Object extractValue(String recordStr, FieldTO fieldTO) {
		String valueStr = recordStr.substring(fieldTO.getStart()-1, fieldTO.getStart()-1+fieldTO.getSize());
		Object value = Field.getInstance(valueStr, fieldTO.getType()).getValue();
		return value;
	}
}
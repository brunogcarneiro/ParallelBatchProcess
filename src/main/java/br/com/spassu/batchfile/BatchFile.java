package br.com.spassu.batchfile;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import br.com.spassu.xml.FieldTO;
import br.com.spassu.xml.RecordTO;

public class BatchFile {
	private RecordTO recordLayout;
	private BufferedReader br;
	private int maxFieldSize;
	
	public BatchFile(RecordTO recordLayout, String filePath) throws IOException {
		this.recordLayout = recordLayout;
		this.maxFieldSize = calculateMaxFieldSize(recordLayout);
		
		this.br = Files.newBufferedReader(Paths.get(filePath));
	}

	private int calculateMaxFieldSize(RecordTO recordLayout) {
		int maxFieldSize = 0;
		
		for (FieldTO field : recordLayout.getFields()) {
			if (field.getSize() > maxFieldSize) {
				maxFieldSize = field.getSize();
			}
		}
		
		return maxFieldSize;
	}

	
}

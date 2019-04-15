package br.com.spassu.parallelbatchprocess.read;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import br.com.spassu.parallelbatchprocess.ProcessState;
import br.com.spassu.parallelbatchprocess.read.xml.FieldTO;
import br.com.spassu.parallelbatchprocess.read.xml.RecordTO;

public class TextReader implements Reader {
	BufferedReader br;
	RecordTO recordLayout;
	int recordLength;
	ProcessState state = ProcessState.NOT_STARTED;

	public TextReader(String path, RecordTO recordLayout) throws IOException {
		br = Files.newBufferedReader(Paths.get(path));
		this.recordLayout = recordLayout;
		this.recordLength = calculateRecordSize(recordLayout);
	}
	
	public Stream<String> recordStream() {
        Iterator<String> iter = new Iterator<String>() {
            char[] nextLine;

            public boolean hasNext() {
                if (nextLine != null) {
                    return true;
                } else {
                    try {
                    	nextLine = new char[recordLength];
                        br.read(nextLine,0,recordLength);
                        br.readLine(); //descarta o restante da linha
                        return (nextLine != null);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
            }

            public String next() {
                if (nextLine != null || hasNext()) {
                    String line = new String(nextLine);
                    nextLine = null;
                    return line;
                } else {
                    throw new NoSuchElementException();
                }
            }
        };
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                iter, Spliterator.ORDERED | Spliterator.NONNULL), false);
    }
	
	private int calculateRecordSize(RecordTO recordLayout) {
		int recordSize = recordLayout.getFields().stream()
							.mapToInt(FieldTO::getSize)
							.sum();

		return recordSize;
	}
	
	private Map<String, String> createRecordMap(String recordStr) {	
		return recordLayout
			.getFields()
			.stream()
			.collect(
				Collectors.toMap(
					FieldTO::getName,
					fieldTO -> recordStr.substring(fieldTO.getStart()-1, fieldTO.getStart()-1+fieldTO.getSize())
				)
			);
	}

	@Override
	public Stream<Map<String, String>> getRecordsMap() {
		return recordStream()
			.map(this::createRecordMap);
	}

}

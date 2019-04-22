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
	private int DELAY = 0;
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
        	int nextLineLength = -1;
            char[] nextLine = new char[recordLength];

            public boolean hasNext() {
                if (nextLineLength > -1) {
                    return true;
                } else {
                    try {
                    	//nextLineLength = -1;
                    	nextLineLength = br.read(nextLine,0,recordLength); //read will return -1 if end of file was found, otherwise, the quantity of read characters.
                        br.readLine(); //descarta o restante da linha
                        return (nextLineLength > -1);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
            }

            public String next() {
                if (nextLine != null || hasNext()) {
                    String line = new String(nextLine);
                    nextLineLength = -1;
                    
                    delay();//Used to simulate communication overhead. Default is 0 ms;
                    
                    return line;
                } else {
                    throw new NoSuchElementException();
                }
            }

			private void delay() {
				if(DELAY < 1) return;
				
				try {
					Thread.sleep(DELAY);
				} catch (InterruptedException e) {
					e.printStackTrace();
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
	public Stream<String> getRecordsMap() {
		return recordStream();
			//.map(this::createRecordMap);
	}

	@Override
	public void setDelay(int miliseconds) {
		DELAY = miliseconds;
	}
	
	public Stream<String> lines() {
		return br.lines();
				//.map(this::createRecordMap);
	}
}

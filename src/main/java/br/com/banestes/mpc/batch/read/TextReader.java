package br.com.banestes.mpc.batch.read;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import br.com.banestes.mpc.batch.ProcessState;
import br.com.banestes.mpc.batch.parse.Parser;
import br.com.banestes.mpc.batch.read.xml.FieldTO;
import br.com.banestes.mpc.batch.read.xml.RecordTO;

public class TextReader implements Reader {
	private int DELAY = 0;
	BufferedReader br;
	RecordTO recordLayout;
	int recordLength;
	ProcessState state = ProcessState.NOT_STARTED;
	
	Parser parser;

	public TextReader(String path, RecordTO recordLayout) throws IOException {
		this(Paths.get(path), recordLayout);
	}
	
	public TextReader(Path path, RecordTO recordLayout) throws IOException {
		br = Files.newBufferedReader(path);
		this.recordLayout = recordLayout;
		this.recordLength = calculateRecordSize(recordLayout);
	}
	
	public Stream<String> recordStream() {
        Iterator<String> iter = new Iterator<String>() {
        	int nextLineLength = -1;
            char[] nextLine = new char[recordLength];
            
            int count = 0;

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
            	
            	/*if (count++ > 500) {
            		throw new RuntimeException("Test exception in Reader");
            	}*/
            	
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

	@Override
	public Stream<String> getRecordsMap() {
		return recordStream();
	}

	@Override
	public void setDelay(int miliseconds) {
		DELAY = miliseconds;
	}
	
	public Stream<String> lines() {
		return br.lines();
	}

	@Override
	public ProcessState getState() {
		return this.state;
	}

	@Override
	public void setParser(Parser parser) {
		this.parser = parser;
	}

	@Override
	public Boolean call() throws Exception {
		this.state = ProcessState.RUNNING;
		
		getRecordsMap()
		 .forEach(parser::pushReadItem);
		
		this.state = ProcessState.DONE;
		
		return true;
	}
}

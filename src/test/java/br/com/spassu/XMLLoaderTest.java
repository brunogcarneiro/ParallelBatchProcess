package br.com.spassu;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import br.com.spassu.parallelbatchprocess.ParallelBatchProcess;
import br.com.spassu.parallelbatchprocess.parse.GenericParser;
import br.com.spassu.parallelbatchprocess.parse.Parser;
import br.com.spassu.parallelbatchprocess.read.Reader;
import br.com.spassu.parallelbatchprocess.read.TextReader;
import br.com.spassu.parallelbatchprocess.read.xml.LayoutTO;
import br.com.spassu.parallelbatchprocess.read.xml.XMLLoader;

/**
 * Unit test for simple App.
 */
@TestInstance(Lifecycle.PER_CLASS)
public class XMLLoaderTest
{
	LayoutTO layout;
	ParallelBatchProcess myBatch;
	
	@Test
	@BeforeAll
    public void loadXML() throws Exception
    {
		layout = XMLLoader.loadLayoutFromXML("layout2.xml");
        assertEquals(layout.getRecords().size(), 1);
        assertEquals(layout.getRecord("0").getFields().size(), 74);
        
      //System.out.println(new File(".").getCanonicalPath());
        String testResourcePath = "src/test/resources/";
        Reader myReader = new TextReader(testResourcePath+"layout/example1.txt", layout.getRecord("0"));
        Parser myParser = new GenericParser();
       
        myBatch = new ParallelBatchProcess(myReader, myParser, layout.getRecord("0"));
        
        assertTrue(true);
    }
	
	@Test
    public void start() throws Exception
    {
        myBatch.start();
    }
}

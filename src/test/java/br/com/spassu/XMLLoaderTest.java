package br.com.spassu;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;

import org.junit.jupiter.api.Test;

import br.com.spassu.batchfile.BatchFile;
import br.com.spassu.batchfile.xml.LayoutTO;
import br.com.spassu.batchfile.xml.XMLLoader;

/**
 * Unit test for simple App.
 */
public class XMLLoaderTest
{
	@Test
    public void loadXML() throws Exception
    {
		LayoutTO layout = XMLLoader.loadLayoutFromXML("layout2.xml");
        assertEquals(layout.getRecords().size(), 1);
        assertEquals(layout.getRecord("0").getFields().size(), 74);
    }
	
	@Test
    public void readStringStream() throws Exception
    {
		LayoutTO layout = XMLLoader.loadLayoutFromXML("layout2.xml");
        assertEquals(layout.getRecords().size(), 1);
        assertEquals(layout.getRecord("0").getFields().size(), 74);
        
        System.out.println(new File(".").getCanonicalPath());
        String testResourcePath = "src/test/resources/";
        BatchFile bf = new BatchFile(layout.getRecord("0"), testResourcePath+"layout/example1.txt");
        bf.printRecords();
        assertTrue(true);
    }
}

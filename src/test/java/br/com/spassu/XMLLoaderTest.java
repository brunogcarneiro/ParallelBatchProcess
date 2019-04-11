package br.com.spassu;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import br.com.spassu.xml.LayoutTO;
import br.com.spassu.xml.XMLLoader;

/**
 * Unit test for simple App.
 */
public class XMLLoaderTest
{
	@Test
    public void loadXML() throws Exception
    {
		LayoutTO layout = XMLLoader.loadLayoutFromXML("myLayout.xml");
        assertEquals(layout.getRecords().size(), 7);
        assertEquals(layout.getRecord("0").getFields().size(), 25);
    }
}
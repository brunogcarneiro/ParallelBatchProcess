package br.com.spassu;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import br.com.spassu.parallelbatchprocess.ParallelBatchProcess;
import br.com.spassu.parallelbatchprocess.parse.GenericParser;
import br.com.spassu.parallelbatchprocess.parse.Parser;
import br.com.spassu.parallelbatchprocess.writer.OracleWriter;
import br.com.spassu.parallelbatchprocess.writer.RelationalDBWriter;
import br.com.spassu.parallelbatchprocess.writer.Writer;
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
	Reader myReader;
	Parser myParser;
	Writer myWriter;
	
	//@Test
	@BeforeAll
    public void loadXML() throws Exception
    {
		layout = XMLLoader.loadLayoutFromXML("layout2.xml");
        assertEquals(layout.getRecords().size(), 1);
        assertEquals(layout.getRecord("0").getFields().size(), 74);
        
        //System.out.println(new File(".").getCanonicalPath());
        String testResourcePath = "src/test/resources/";
        myReader = new TextReader(testResourcePath+"layout/example1.txt", layout.getRecord("0"));
        myParser = new GenericParser();
      //  myWriter = new RelationalDBWriter("jdbc:h2:~/pc","spassu","123");
        try {
        myWriter = new OracleWriter("jdbc:oracle:thin:@10.8.8.40:1521:HML02","pmpce","pmpce001",layout);
        } catch (Exception e) {
        	System.out.println(e.getMessage() + e.getCause());
        	e.printStackTrace();
        }
        
        
		/*try {
			//myWriter.cleanTable();
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}*/
       
        myBatch = new ParallelBatchProcess(myReader, myParser, layout.getRecord("0"), myWriter);
        
        assertTrue(true);
    }
	
	@Test
    public void start() throws Exception
    {
       myBatch.start();
    }
	
	//@Test
    public void writerTest() throws Exception
    {
		Map<String, Object> parsedRecord1 = Stream.of(
	            new SimpleEntry<>("CID_RESPONSAVEL", truncate("CID_RESPONSAVEL",4)),
	            new SimpleEntry<>("CID_ELABORACAO", truncate("CID_ELABORACAO",4)),
	            new SimpleEntry<>("CPF_CNPJ", truncate("CPF_CNPJ",14)),
	            new SimpleEntry<>("CONTA", truncate("CONTA",10)),
	            new SimpleEntry<>("ANO", truncate("ANO",4)),
	            new SimpleEntry<>("NUM_PROPOSTA", truncate("NUM_PROPOSTA",7)),
	            new SimpleEntry<>("DATA_INICIAL_PN", truncate("DATA_INICIAL_PN",8)),
	            new SimpleEntry<>("VLR_PROPOSTA", new BigDecimal("10000")),
	            new SimpleEntry<>("SIT_PN", truncate("SIT_PN",1)),
	            new SimpleEntry<>("ALCADA", truncate("ALCADA",2)),
	            new SimpleEntry<>("DATA_FORMALIZACAO", truncate("DATA_FORMALIZACAO",8)),
	            new SimpleEntry<>("DATA_APROVACAO", truncate("DATA_APROVACAO",8)),
	            new SimpleEntry<>("DATA_EFETIVACAO", truncate("DATA_EFETIVACAO",8)),
	            new SimpleEntry<>("COD_PRODUTO", truncate("COD_PRODUTO",4)),
	            new SimpleEntry<>("COD_SUBPROD", truncate("COD_SUBPROD",4)),
	            new SimpleEntry<>("MATR_PROPONENTE", truncate("MATR_PROPONENTE",9)),
	            new SimpleEntry<>("MATR_RESP_FORM", truncate("MATR_RESP_FORM",9)),
	            new SimpleEntry<>("MATR_RESP_LIB", truncate("MATR_RESP_LIB",9)),
	            new SimpleEntry<>("TX_PROPOSTA", truncate("TX_PROPOSTA",9)),
	            new SimpleEntry<>("INDENT_TX", truncate("INDENT_TX",1)),
	            new SimpleEntry<>("PRAZO", truncate("PRAZO",4)),
	            new SimpleEntry<>("IDENT_PRAZO", truncate("IDENT_PRAZO",1)),
	            new SimpleEntry<>("CONT_ORIGEM", truncate("CONT_ORIGEM",20)),
	            new SimpleEntry<>("QDE_PARC", truncate("QDE_PARC",4)),
	            new SimpleEntry<>("DT_VENC_CONT_PROR", truncate("DT_VENC_CONT_PROR",8)),
	            new SimpleEntry<>("DT_RENEG", truncate("DT_RENEG",8)),
	            new SimpleEntry<>("PERC_CAC", truncate("PERC_CAC",5)),
	            new SimpleEntry<>("CLASS_PN", truncate("CLASS_PN",2)),
	            new SimpleEntry<>("PERC_POS_FIX", truncate("PERC_POS_FIX",5)),
	            new SimpleEntry<>("COD_POS_FIX", truncate("COD_POS_FIX",4)),
	            new SimpleEntry<>("RESSALVA_PN", truncate("RESSALVA_PN",1)),
	            new SimpleEntry<>("IND_VOTO_NEG", truncate("IND_VOTO_NEG",1)),
	            new SimpleEntry<>("ORIGEM_PN", truncate("ORIGEM_PN",1)),
	            new SimpleEntry<>("IND_AN_LIBER", truncate("IND_AN_LIBER",1)),
	            new SimpleEntry<>("TX_NEGOCIADA", truncate("TX_NEGOCIADA",7)),
	            new SimpleEntry<>("COD_EXC1", truncate("COD_EXC1",4)),
	            new SimpleEntry<>("COD_EXC2", truncate("COD_EXC2",4)),
	            new SimpleEntry<>("COD_EXC3", truncate("COD_EXC3",4)),
	            new SimpleEntry<>("COD_EXC4", truncate("COD_EXC4",4)),
	            new SimpleEntry<>("COD_EXC5", truncate("COD_EXC5",4)),
	            new SimpleEntry<>("COD_EXC6", truncate("COD_EXC6",4)),
	            new SimpleEntry<>("COD_EXC7", truncate("COD_EXC7",4)),
	            new SimpleEntry<>("COD_EXC8", truncate("COD_EXC8",4)),
	            new SimpleEntry<>("COD_EXC9", truncate("COD_EXC9",4)),
	            new SimpleEntry<>("COD_EXC10", truncate("COD_EXC10",4)),
	            new SimpleEntry<>("COD_EXC11", truncate("COD_EXC11",4)),
	            new SimpleEntry<>("COD_EXC12", truncate("COD_EXC12",4)),
	            new SimpleEntry<>("COD_EXC13", truncate("COD_EXC13",4)),
	            new SimpleEntry<>("COD_EXC14", truncate("COD_EXC14",4)),
	            new SimpleEntry<>("COD_EXC15", truncate("COD_EXC15",4)),
	            new SimpleEntry<>("COD_EXC16", truncate("COD_EXC16",4)),
	            new SimpleEntry<>("COD_EXC17", truncate("COD_EXC17",4)),
	            new SimpleEntry<>("COD_EXC18", truncate("COD_EXC18",4)),
	            new SimpleEntry<>("COD_EXC19", truncate("COD_EXC19",4)),
	            new SimpleEntry<>("COD_EXC20", truncate("COD_EXC20",4)),
	            new SimpleEntry<>("COD_EXC21", truncate("COD_EXC21",4)),
	            new SimpleEntry<>("COD_EXC22", truncate("COD_EXC22",4)),
	            new SimpleEntry<>("COD_EXC23", truncate("COD_EXC23",4)),
	            new SimpleEntry<>("COD_EXC24", truncate("COD_EXC24",4)),
	            new SimpleEntry<>("COD_EXC25", truncate("COD_EXC25",4)),
	            new SimpleEntry<>("COD_EXC26", truncate("COD_EXC26",4)),
	            new SimpleEntry<>("COD_EXC27", truncate("COD_EXC27",4)),
	            new SimpleEntry<>("COD_EXC28", truncate("COD_EXC28",4)),
	            new SimpleEntry<>("COD_EXC29", truncate("COD_EXC29",4)),
	            new SimpleEntry<>("COD_EXC30", truncate("COD_EXC30",4)),
	            new SimpleEntry<>("CPF_CNPJ_AVALISTA1", truncate("CPF_CNPJ_AVALISTA1",14)),
	            new SimpleEntry<>("ID_AVALISTA1", truncate("ID_AVALISTA1",1)),
	            new SimpleEntry<>("CPF_CNPJ_AVALISTA2", truncate("CPF_CNPJ_AVALISTA2",14)),
	            new SimpleEntry<>("ID_AVALISTA2", truncate("ID_AVALISTA2",1)),
	            new SimpleEntry<>("CPF_CNPJ_AVALISTA3", truncate("CPF_CNPJ_AVALISTA3",14)),
	            new SimpleEntry<>("ID_AVALISTA3", truncate("ID_AVALISTA3",1)),
	            new SimpleEntry<>("CPF_CNPJ_AVALISTA4", truncate("CPF_CNPJ_AVALISTA4",14)),
	            new SimpleEntry<>("ID_AVALISTA4", truncate("ID_AVALISTA4",1)),
	            new SimpleEntry<>("GARANTIAS", truncate("GARANTIAS",24))
	    ).collect(Collectors.toMap(Entry::getKey, Entry::getValue));
		
		Map<String, Object> parsedRecord2 = Stream.of(
	            new SimpleEntry<>("CID_RESPONSAVEL", truncate("CID_RESPONSAVEL",4)),
	            new SimpleEntry<>("CID_ELABORACAO", truncate("CID_ELABORACAO",4)),
	            new SimpleEntry<>("CPF_CNPJ", truncate("11111",14)),
	            new SimpleEntry<>("CONTA", truncate("CONTA",10)),
	            new SimpleEntry<>("ANO", truncate("ANO",4)),
	            new SimpleEntry<>("NUM_PROPOSTA", truncate("NUM_PROPOSTA",7)),
	            new SimpleEntry<>("DATA_INICIAL_PN", truncate("DATA_INICIAL_PN",8)),
	            new SimpleEntry<>("VLR_PROPOSTA", new BigDecimal("10000")),
	            new SimpleEntry<>("SIT_PN", truncate("SIT_PN",1)),
	            new SimpleEntry<>("ALCADA", truncate("ALCADA",2)),
	            new SimpleEntry<>("DATA_FORMALIZACAO", truncate("DATA_FORMALIZACAO",8)),
	            new SimpleEntry<>("DATA_APROVACAO", truncate("DATA_APROVACAO",8)),
	            new SimpleEntry<>("DATA_EFETIVACAO", truncate("DATA_EFETIVACAO",8)),
	            new SimpleEntry<>("COD_PRODUTO", truncate("COD_PRODUTO",4)),
	            new SimpleEntry<>("COD_SUBPROD", truncate("COD_SUBPROD",4)),
	            new SimpleEntry<>("MATR_PROPONENTE", truncate("MATR_PROPONENTE",9)),
	            new SimpleEntry<>("MATR_RESP_FORM", truncate("MATR_RESP_FORM",9)),
	            new SimpleEntry<>("MATR_RESP_LIB", truncate("MATR_RESP_LIB",9)),
	            new SimpleEntry<>("TX_PROPOSTA", truncate("TX_PROPOSTA",9)),
	            new SimpleEntry<>("INDENT_TX", truncate("INDENT_TX",1)),
	            new SimpleEntry<>("PRAZO", truncate("PRAZO",4)),
	            new SimpleEntry<>("IDENT_PRAZO", truncate("IDENT_PRAZO",1)),
	            new SimpleEntry<>("CONT_ORIGEM", truncate("CONT_ORIGEM",20)),
	            new SimpleEntry<>("QDE_PARC", truncate("QDE_PARC",4)),
	            new SimpleEntry<>("DT_VENC_CONT_PROR", truncate("DT_VENC_CONT_PROR",8)),
	            new SimpleEntry<>("DT_RENEG", truncate("DT_RENEG",8)),
	            new SimpleEntry<>("PERC_CAC", truncate("PERC_CAC",5)),
	            new SimpleEntry<>("CLASS_PN", truncate("CLASS_PN",2)),
	            new SimpleEntry<>("PERC_POS_FIX", truncate("PERC_POS_FIX",5)),
	            new SimpleEntry<>("COD_POS_FIX", truncate("COD_POS_FIX",4)),
	            new SimpleEntry<>("RESSALVA_PN", truncate("RESSALVA_PN",1)),
	            new SimpleEntry<>("IND_VOTO_NEG", truncate("IND_VOTO_NEG",1)),
	            new SimpleEntry<>("ORIGEM_PN", truncate("ORIGEM_PN",1)),
	            new SimpleEntry<>("IND_AN_LIBER", truncate("IND_AN_LIBER",1)),
	            new SimpleEntry<>("TX_NEGOCIADA", truncate("TX_NEGOCIADA",7)),
	            new SimpleEntry<>("COD_EXC1", truncate("COD_EXC1",4)),
	            new SimpleEntry<>("COD_EXC2", truncate("COD_EXC2",4)),
	            new SimpleEntry<>("COD_EXC3", truncate("COD_EXC3",4)),
	            new SimpleEntry<>("COD_EXC4", truncate("COD_EXC4",4)),
	            new SimpleEntry<>("COD_EXC5", truncate("COD_EXC5",4)),
	            new SimpleEntry<>("COD_EXC6", truncate("COD_EXC6",4)),
	            new SimpleEntry<>("COD_EXC7", truncate("COD_EXC7",4)),
	            new SimpleEntry<>("COD_EXC8", truncate("COD_EXC8",4)),
	            new SimpleEntry<>("COD_EXC9", truncate("COD_EXC9",4)),
	            new SimpleEntry<>("COD_EXC10", truncate("COD_EXC10",4)),
	            new SimpleEntry<>("COD_EXC11", truncate("COD_EXC11",4)),
	            new SimpleEntry<>("COD_EXC12", truncate("COD_EXC12",4)),
	            new SimpleEntry<>("COD_EXC13", truncate("COD_EXC13",4)),
	            new SimpleEntry<>("COD_EXC14", truncate("COD_EXC14",4)),
	            new SimpleEntry<>("COD_EXC15", truncate("COD_EXC15",4)),
	            new SimpleEntry<>("COD_EXC16", truncate("COD_EXC16",4)),
	            new SimpleEntry<>("COD_EXC17", truncate("COD_EXC17",4)),
	            new SimpleEntry<>("COD_EXC18", truncate("COD_EXC18",4)),
	            new SimpleEntry<>("COD_EXC19", truncate("COD_EXC19",4)),
	            new SimpleEntry<>("COD_EXC20", truncate("COD_EXC20",4)),
	            new SimpleEntry<>("COD_EXC21", truncate("COD_EXC21",4)),
	            new SimpleEntry<>("COD_EXC22", truncate("COD_EXC22",4)),
	            new SimpleEntry<>("COD_EXC23", truncate("COD_EXC23",4)),
	            new SimpleEntry<>("COD_EXC24", truncate("COD_EXC24",4)),
	            new SimpleEntry<>("COD_EXC25", truncate("COD_EXC25",4)),
	            new SimpleEntry<>("COD_EXC26", truncate("COD_EXC26",4)),
	            new SimpleEntry<>("COD_EXC27", truncate("COD_EXC27",4)),
	            new SimpleEntry<>("COD_EXC28", truncate("COD_EXC28",4)),
	            new SimpleEntry<>("COD_EXC29", truncate("COD_EXC29",4)),
	            new SimpleEntry<>("COD_EXC30", truncate("COD_EXC30",4)),
	            new SimpleEntry<>("CPF_CNPJ_AVALISTA1", truncate("CPF_CNPJ_AVALISTA1",14)),
	            new SimpleEntry<>("ID_AVALISTA1", truncate("ID_AVALISTA1",1)),
	            new SimpleEntry<>("CPF_CNPJ_AVALISTA2", truncate("CPF_CNPJ_AVALISTA2",14)),
	            new SimpleEntry<>("ID_AVALISTA2", truncate("ID_AVALISTA2",1)),
	            new SimpleEntry<>("CPF_CNPJ_AVALISTA3", truncate("CPF_CNPJ_AVALISTA3",14)),
	            new SimpleEntry<>("ID_AVALISTA3", truncate("ID_AVALISTA3",1)),
	            new SimpleEntry<>("CPF_CNPJ_AVALISTA4", truncate("CPF_CNPJ_AVALISTA4",14)),
	            new SimpleEntry<>("ID_AVALISTA4", truncate("ID_AVALISTA4",1)),
	            new SimpleEntry<>("GARANTIAS", truncate("GARANTIAS",24))
	    ).collect(Collectors.toMap(Entry::getKey, Entry::getValue));
		
		List<Map<String, Object>> parsedData = new LinkedList<>();
		parsedData.add(parsedRecord1);
		parsedData.add(parsedRecord2);
		
		//myWriter.write(parsedData);
    }
	
	private String truncate(String string, int i) {
		if (string.length() > i) {
			string = string.substring(0,i);
		} else {
			while (string.length() < i)
			string = string.concat("1");
		}
		
		return string;
	}

	@AfterAll
	public void closeConnection() {
		myWriter.close();
	}
}

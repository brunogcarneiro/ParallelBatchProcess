package br.com.spassu.batchfile.xml;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.Serializable;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.DomDriver;
import com.thoughtworks.xstream.security.NullPermission;

public class XMLLoader implements Serializable
{

  private static final long serialVersionUID = 1L;

  /**
   * @param xmlFileName
   * @return
   * @throws Exception
   */
  public static LayoutTO loadLayoutFromXML(InputStream inputStream) throws Exception
  {
    // Carregar layout
    XStream stream = new XStream(new DomDriver());
    stream.addPermission(NullPermission.NULL);

    stream.alias("layout", LayoutTO.class);
    stream.alias("record", RecordTO.class);
    stream.alias("field", FieldTO.class);

    stream.addImplicitCollection(LayoutTO.class, "records");
    stream.addImplicitCollection(RecordTO.class, "fields");

    stream.useAttributeFor(String.class);
    stream.useAttributeFor(int.class);
    stream.useAttributeFor(boolean.class);
    stream.useAttributeFor(Integer.class);

    LayoutTO layoutTO = (LayoutTO) stream.fromXML(inputStream);
    layoutTO.validate();

    return layoutTO;
  }

  /**
   * @param xmlFileName
   * @return
   * @throws Exception
   */
  public static LayoutTO loadLayoutFromXML(String xmlFileName) throws Exception
  {
    // Verificar classe no CLASSPATH
    try
    {
      Thread.currentThread().getContextClassLoader().loadClass("com.thoughtworks.xstream.XStream");
      // Class.forName("com.thoughtworks.xstream.XStream");
    } catch (ClassNotFoundException ex)
    {
      throw new Exception("A classe 'com.thoughtworks.xstream.XStream' nao esta no CLASSPATH");
    }

    // Recuperar separador do SO
    String separator = System.getProperty("file.separator");

    // Recuperar arquivo com definicao
    InputStream inputStream = null;
    if (xmlFileName.indexOf(separator) == -1)
    {
      inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("layout/" + xmlFileName);
    } else
    {
      inputStream = new FileInputStream(xmlFileName);
    }

    if (inputStream == null)
    {
      throw new Exception("Arquivo [" + xmlFileName + "] nao encontrado");
    }

    return loadLayoutFromXML(inputStream);
  }
}

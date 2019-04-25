package br.com.banestes.mpc.batch.read.xml;


import java.io.Serializable;
import java.util.LinkedList;

/**
 * 
 * @author guilherme.santos
 *
 */
public class RecordTO implements Serializable
{

  private static final long serialVersionUID = 1L;

  private LinkedList<FieldTO> fields;

  private String identifier;

  private String obs;

  public RecordTO()
  {
    this.fields = new LinkedList<FieldTO>();
  }

  /**
   * 
   * @param identifier
   */
  public RecordTO(String identifier)
  {
    this.identifier = identifier;
    this.fields = new LinkedList<FieldTO>();
  }

  /**
   * 
   * @return
   */
  public String getIdentifier()
  {
    return this.identifier;
  }

  /**
   * @param identifier O identifier a ser definido.
   */
  public void setIdentifier(String identifier)
  {
    this.identifier = identifier;
  }

  /**
   * @param fields O fields a ser definido.
   */
  public void setFields(LinkedList<FieldTO> fields)
  {
    this.fields = fields;
  }

  /**
   * 
   * @return
   */
  public LinkedList<FieldTO> getFields()
  {
    if (this.fields == null)
    {
      this.fields = new LinkedList<FieldTO>();
    }
    return this.fields;
  }

  /**
   * 
   * @param size
   */
  public void addAlfaNumericField(String name, int size)
  {
    this.fields.add(new FieldTO(name, size, FieldTO._TP_ALFANUMERIC));
  }

  /**
   * 
   * @param size
   */
  public void addAlfaNumericFiller(int size)
  {
    this.fields.add(new FieldTO(FieldTO._filler, size, FieldTO._TP_ALFANUMERIC, true));
  }

  /**
   * 
   * @param size
   */
  public void addNumericField(String name, int size)
  {
    this.fields.add(new FieldTO(name, size, FieldTO._TP_NUMERIC));
  }

  /**
   * 
   * @param size
   */
  public void addNumericFiller(int size)
  {
    this.fields.add(new FieldTO(FieldTO._filler, size, FieldTO._TP_NUMERIC, true));
  }

  /**
   * @return Retorna o obs.
   */
  public String getObs()
  {
    return this.obs;
  }

  /**
   * @param obs O obs a ser definido.
   */
  public void setObs(String obs)
  {
    this.obs = obs;
  }

  /**
   * 
   * @param name
   * @param size
   * @param format
   */
  public void addDateTimeField(String name, int size, String format)
  {
    this.fields.add(new FieldTO(name, size, FieldTO._TP_DATE_TIME, format));
  }

  /*
   * (não-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString()
  {
    StringBuilder builder = new StringBuilder();
    builder.append("RecordTO[identifier:").append(this.identifier);
    builder.append(";obs:").append(this.obs);
    builder.append(";");
    for (FieldTO fieldTO : this.getFields())
    {
      builder.append(fieldTO);
    }
    builder.append("]");
    return builder.toString();
  }

}
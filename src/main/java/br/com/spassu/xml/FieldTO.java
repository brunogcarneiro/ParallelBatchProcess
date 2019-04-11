package br.com.spassu.xml;

import java.io.Serializable;

/**
 * 
 * @author guilherme.santos
 *
 */
public class FieldTO implements Serializable
{

  private static final long serialVersionUID = 1L;

  public static final String _yyyyMMdd = "yyyyMMdd";

  public static final String _ddMMyyyy = "ddMMyyyy";

  public static final String _HHmmss = "HHmmss";

  public static final String _filler = "filler";

  public static final String _TP_ALFANUMERIC = "A";

  public static final String _TP_NUMERIC = "N";

  public static final String _TP_DATE_TIME = "D";

  private String name;

  private int size;

  private String type;

  private boolean filler;

  private String format;

  private boolean convertException;

  private Integer scale;

  private String fieldRecordRef;

  private String obs;

  private boolean completeToSize;

  private String description;

  public FieldTO()
  {

  }

  /**
   * @param size
   * @param format
   */
  public FieldTO(String name, int size, String type)
  {
    this.name = name;
    this.size = size;
    this.type = type;
    this.format = null;
    this.filler = false;
  }

  /**
   * @param size
   * @param type
   * @param format
   */
  public FieldTO(String name, int size, String type, String format)
  {
    this(name, size, type);
    this.format = format;
  }

  /**
   * 
   * @param name
   * @param size
   * @param type
   * @param filler
   */
  public FieldTO(String name, int size, String type, boolean filler)
  {
    this(name, size, type);
    this.filler = filler;
  }

  /**
   * 
   * @param name
   * @param size
   * @param type
   * @param format
   * @param filler
   */
  public FieldTO(String name, int size, String type, String format, boolean filler)
  {
    this(name, size, type, format);
    this.filler = filler;
  }

  /**
   * @return Retorna o tamanho.
   */
  public int getSize()
  {
    return this.size;
  }

  /**
   * @param tamanho O tamanho a ser definido.
   */
  public void setSize(int tamanho)
  {
    this.size = tamanho;
  }

  /**
   * @return Retorna o tipo.
   */
  public String getType()
  {
    return this.type;
  }

  /**
   * @param tipo O tipo a ser definido.
   */
  public void setType(String tipo)
  {
    this.type = tipo;
  }

  /**
   * @return Retorna o formato.
   */
  public String getFormat()
  {
    return this.format;
  }

  /**
   * @param formato O formato a ser definido.
   */
  public void setFormat(String formato)
  {
    this.format = formato;
  }

  /**
   * @return Retorna o nome.
   */
  public String getName()
  {
    return this.name;
  }

  /**
   * @param nome O nome a ser definido.
   */
  public void setName(String nome)
  {
    this.name = nome;
  }

  /**
   * @return Retorna o filler.
   */
  public boolean isFiller()
  {
    return this.filler;
  }

  /**
   * @param filler O filler a ser definido.
   */
  public void setFiller(boolean filler)
  {
    this.filler = filler;
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
    builder.append("FieldTO[name:").append(this.name);
    builder.append(";size:").append(this.size);
    builder.append(";type:").append(this.type);
    builder.append(";filler:").append(this.filler);
    builder.append(";format:").append(this.format);
    builder.append(";scale:").append(this.scale);
    builder.append(";fieldRecordRef:").append(this.fieldRecordRef);
    builder.append(";obs:").append(this.obs);
    builder.append(";completeToSize:").append(this.completeToSize);
    builder.append("]");
    return builder.toString();
  }

  /**
   * @return Retorna o scale.
   */
  public Integer getScale()
  {
    return this.scale;
  }

  /**
   * @param scale O scale a ser definido.
   */
  public void setScale(Integer scale)
  {
    this.scale = scale;
  }

  /**
   * @return Retorna o fieldRecordRef.
   */
  public String getFieldRecordRef()
  {
    return this.fieldRecordRef;
  }

  /**
   * @param fieldRecordRef O fieldRecordRef a ser definido.
   */
  public void setFieldRecordRef(String fieldRecordRef)
  {
    this.fieldRecordRef = fieldRecordRef;
  }

  public String getObs()
  {
    return this.obs;
  }

  public void setObs(String obs)
  {
    this.obs = obs;
  }

  public boolean isCompleteToSize()
  {
    return this.completeToSize;
  }

  public void setCompleteToSize(boolean completeToSize)
  {
    this.completeToSize = completeToSize;
  }

  /**
   * @return Retorna o desc.
   */
  public String getDescription()
  {
    return this.description;
  }

  /**
   * @param desc O desc a ser definido.
   */
  public void setDescription(String desc)
  {
    this.description = desc;
  }

  /**
   * @return Retorna o lenient.
   */
  public boolean isConvertException()
  {
    return this.convertException;
  }

  /**
   * @param convertException O lenient a ser definido.
   */
  public void setConvertException(boolean convertException)
  {
    this.convertException = convertException;
  }
}

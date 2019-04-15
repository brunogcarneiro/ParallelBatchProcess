package br.com.spassu.parallelbatchprocess.read.xml;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * 
 * @author guilherme.santos
 *
 */
public class LayoutTO implements Serializable
{

  private static final long serialVersionUID = 1L;

  private String tableName;

  private String poolName;

  private int commitRows;

  private boolean truncate;

  private ArrayList<RecordTO> records;

  /**
   * @param tamanhoIdentificador
   */
  public LayoutTO()
  {
    this.records = new ArrayList<RecordTO>();
  }

  /**
   * @return Retorna o records.
   */
  public ArrayList<RecordTO> getRecords()
  {
    if (this.records == null)
    {
      this.records = new ArrayList<RecordTO>();
    }
    return this.records;
  }

  /**
   * @param records O records a ser definido.
   */
  public void setRecords(ArrayList<RecordTO> records)
  {
    this.records = records;
  }

  /**
   * @param recordTO
   * @throws Exception
   */
  public void addRecord(RecordTO recordTO)
  {
    this.records.add(recordTO);
  }

  /**
   * @param identificador
   * @return
   */
  public RecordTO getRecord(String identificador)
  {
    for (RecordTO recordTO : this.getRecords())
    {
      if (recordTO.getIdentifier().equals(identificador))
      {
        return recordTO;
      }
    }
    return null;
  }

  /**
   * Validar campo obrigatorios do layout
   * 
   * @throws Exception
   */
  public void validate() throws Exception
  {
    StringBuilder errors = new StringBuilder();

    if ((this.getTableName() != null) && (this.getPoolName() == null))
    {
      errors.append("Atributo tableName e poolName devem ser obrigatóriodos, se definido um ou outro\n");
    }
    if ((this.getTableName() == null) && (this.getPoolName() != null))
    {
      errors.append("Atributo tableName e poolName devem ser obrigatóriodos, se definido um ou outro\n");
    }

    for (RecordTO recordTO : this.getRecords())
    {
      if (recordTO.getIdentifier() == null)
      {
        errors.append("Identificador do registro não esta definido\n");
      }

      for (FieldTO fieldTO : recordTO.getFields())
      {
        if (fieldTO.getName() == null)
        {
          errors.append("Nome do campo não está definido\n");
        }
        if (fieldTO.getSize() == 0)
        {
          errors.append("Tamanho do campo [" + fieldTO.getName() + "] não está definido\n");
        }
        if ((!fieldTO.getType().equals(FieldTO._TP_ALFANUMERIC)) && (!fieldTO.getType().equals(FieldTO._TP_NUMERIC))
            && (!fieldTO.getType().equals(FieldTO._TP_DATE_TIME)))
        {
          errors.append("Tipo do campo [" + fieldTO.getName() + "] não está definido\n");
        }
      } // for
    } // for

    if (errors.length() != 0)
    {
      throw new Exception(errors.toString());
    }
  }

  @Override
  public String toString()
  {
    StringBuilder builder = new StringBuilder();
    builder.append("LayoutTO[");
    for (RecordTO recordTO : this.getRecords())
    {
      builder.append(recordTO);
    }
    builder.append("]");
    return builder.toString();
  }

  /**
   * @return Retorna o commitRows.
   */
  public int getCommitRows()
  {
    return this.commitRows;
  }

  /**
   * @param commitRows O commitRows a ser definido.
   */
  public void setCommitRows(int commitRows)
  {
    this.commitRows = commitRows;
  }

  /**
   * @return Retorna o poolName.
   */
  public String getPoolName()
  {
    return this.poolName;
  }

  /**
   * @param poolName O poolName a ser definido.
   */
  public void setPoolName(String poolName)
  {
    this.poolName = poolName;
  }

  /**
   * @return Retorna o tableName.
   */
  public String getTableName()
  {
    return this.tableName;
  }

  /**
   * @param tableName O tableName a ser definido.
   */
  public void setTableName(String tableName)
  {
    this.tableName = tableName;
  }

  /**
   * @return Retorna o truncate.
   */
  public boolean getTruncate()
  {
    return this.truncate;
  }

  /**
   * @param truncate O truncate a ser definido.
   */
  public void setTruncate(boolean truncate)
  {
    this.truncate = truncate;
  }

}
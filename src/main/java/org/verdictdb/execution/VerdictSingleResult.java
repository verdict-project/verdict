package org.verdictdb.execution;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.verdictdb.commons.AttributeValueRetrievalHelper;
import org.verdictdb.connection.DbmsQueryResult;

/**
 * Represents the result set returned from VerdictDB to the end user.
 * 
 * @author Yongjoo Park
 *
 */
public class VerdictSingleResult extends AttributeValueRetrievalHelper {

  DbmsQueryResult result;

  public VerdictSingleResult(DbmsQueryResult result) {
    DbmsQueryResult copied = copyResult(result);
    copied.rewind();
    this.result = copied;
  }
  
  private DbmsQueryResult copyResult(DbmsQueryResult result) {
    try {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      ObjectOutputStream out = new ObjectOutputStream(bos);
      out.writeObject(result);
      out.flush();
      out.close();

      ObjectInputStream in = new ObjectInputStream(
          new ByteArrayInputStream(bos.toByteArray()));
      DbmsQueryResult copied = (DbmsQueryResult) in.readObject();
      return copied;
      
    } catch (IOException | ClassNotFoundException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public int getColumnCount() {
    return result.getColumnCount();
  }

  @Override
  public String getColumnName(int index) {
    return result.getColumnName(index);
  }

  public int getColumnType(int index) {
    return result.getColumnType(index);
  }

  public long getRowCount() {
    return result.getRowCount();
  }

  @Override
  public Object getValue(int index) {
    return result.getValue(index);
  }

  public boolean next() {
    return result.next();
  }

  public void rewind() {
    result.rewind();
  }

}

package org.verdictdb.coordinator;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.verdictdb.commons.AttributeValueRetrievalHelper;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.connection.DbmsQueryResultMetaData;

/**
 * Represents the result set returned from VerdictDB to the end user.
 * 
 * @author Yongjoo Park
 *
 */
public class VerdictSingleResult extends AttributeValueRetrievalHelper {

  private DbmsQueryResult result;

  public VerdictSingleResult(DbmsQueryResult result) {
    DbmsQueryResult copied = copyResult(result);
    copied.rewind();
    this.result = copied;
  }

  public VerdictSingleResult(DbmsQueryResult result, boolean asIs) {
    // If result contains objects that cannot be serialized (e.g., BLOB, CLOB in H2),
    // it is just copied as-is (i.e., shallow copy) as opposed to deep copy.
    if (asIs) {
      this.result = result;
    } else {
      DbmsQueryResult copied = copyResult(result);
      copied.rewind();
      this.result = copied;
    }
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

  public DbmsQueryResultMetaData getMetaData() {
    return result.getMetaData();
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

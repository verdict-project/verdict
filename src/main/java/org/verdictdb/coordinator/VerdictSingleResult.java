package org.verdictdb.coordinator;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.verdictdb.commons.AttributeValueRetrievalHelper;
import org.verdictdb.connection.DbmsQueryResult;

import com.google.common.base.Optional;

/**
 * Represents the result set returned from VerdictDB to the end user.
 * 
 * @author Yongjoo Park
 *
 */
public class VerdictSingleResult extends AttributeValueRetrievalHelper {

  Optional<DbmsQueryResult> result;

  public VerdictSingleResult(DbmsQueryResult result) {
    if (result == null) {
      this.result = Optional.absent();
    } else {
      DbmsQueryResult copied = copyResult(result);
      copied.rewind();
      this.result = Optional.of(copied);
    }
  }
  
  public static VerdictSingleResult empty() {
    return new VerdictSingleResult(null);
  }
  
  public boolean isEmpty() {
    return !result.isPresent();
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
    if (result.isPresent() == false) {
      return 0;
    } else {
      return result.get().getColumnCount();
    }
  }

  @Override
  public String getColumnName(int index) {
    if (result.isPresent() == false) {
      throw new RuntimeException("An empty result is accessed.");
    } else {
      return result.get().getColumnName(index);
    }
  }

  public int getColumnType(int index) {
    if (result.isPresent() == false) {
      throw new RuntimeException("An empty result is accessed.");
    } else {
      return result.get().getColumnType(index);
    }
  }

  public long getRowCount() {
    if (result.isPresent() == false) {
      return 0;
    } else {
      return result.get().getRowCount();
    }
  }

  @Override
  public Object getValue(int index) {
    if (result.isPresent() == false) {
      throw new RuntimeException("An empty result is accessed.");
    } else {
      return result.get().getValue(index);
    }
  }

  public boolean next() {
    if (result.isPresent() == false) {
      return false;
    } else {
      return result.get().next();
    }
  }

  public void rewind() {
    if (result.isPresent()) {
      result.get().rewind();
    }
  }

}

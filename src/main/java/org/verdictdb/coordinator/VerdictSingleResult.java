package org.verdictdb.coordinator;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import com.rits.cloning.Cloner;
import org.verdictdb.commons.AttributeValueRetrievalHelper;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.connection.DbmsQueryResultMetaData;

import com.google.common.base.Optional;

/**
 * Represents the result set returned from VerdictDB to the end user.
 * 
 * @author Yongjoo Park
 *
 */
public class VerdictSingleResult extends AttributeValueRetrievalHelper {

  private Optional<DbmsQueryResult> result;

  public VerdictSingleResult(DbmsQueryResult result) {
    if (result == null) {
      this.result = Optional.absent();
    } else {
      DbmsQueryResult copied = copyResult(result);
      copied.rewind();
      this.result = Optional.of(copied);
    }
  }

  public VerdictSingleResult(DbmsQueryResult result, boolean asIs) {
    // If result contains objects that cannot be serialized (e.g., BLOB, CLOB in H2),
    // it is just copied as-is (i.e., shallow copy) as opposed to deep copy.
    if (result == null) {
      this.result = Optional.absent();
    } else {
      if (asIs) {
        this.result = Optional.of(result);
      } else {
        DbmsQueryResult copied = copyResult(result);
        copied.rewind();
        this.result = Optional.of(copied);
      }
    }
  }


  public static VerdictSingleResult empty() {
    return new VerdictSingleResult(null);
  }
  
  public boolean isEmpty() {
    return !result.isPresent();
  }

  private DbmsQueryResult copyResult(DbmsQueryResult result) {
    return new Cloner().deepClone(result);
//    try {
//      ByteArrayOutputStream bos = new ByteArrayOutputStream();
//      ObjectOutputStream out = new ObjectOutputStream(bos);
//      out.writeObject(result);
//      out.flush();
//      out.close();
//
//      ObjectInputStream in = new ObjectInputStream(
//          new ByteArrayInputStream(bos.toByteArray()));
//      DbmsQueryResult copied = (DbmsQueryResult) in.readObject();
//      return copied;
//
//    } catch (ClassNotFoundException e) {
//      e.printStackTrace();
//    } catch (NotSerializableException e) {
//      e.printStackTrace();
//    } catch (IOException e) {
//      e.printStackTrace();
//    }
//    return null;
  }

  public DbmsQueryResultMetaData getMetaData() {
    return result.isPresent() ? result.get().getMetaData() : null;
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

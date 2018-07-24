package org.verdictdb.coordinator;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

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
public abstract class VerdictSingleResult extends AttributeValueRetrievalHelper {

  public abstract boolean isEmpty();

  public abstract long getRowCount();

  public abstract DbmsQueryResultMetaData getMetaData();

  @Override
  public abstract int getColumnCount();

  @Override
  public abstract String getColumnName(int index);

  public abstract int getColumnType(int index);

  @Override
  public abstract Object getValue(int index);

  public abstract boolean wasNull();

  public abstract boolean next();

  public abstract void rewind();

}

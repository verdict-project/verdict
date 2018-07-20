package org.verdictdb.commons;

/**
 * Created by Dong Young Yoon on 7/20/18.
 */
import java.util.UUID;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class UUIDSerializer extends Serializer<UUID> {

  public UUIDSerializer() {
    setImmutable(true);
  }

  @Override
  public void write(final Kryo kryo, final Output output, final UUID uuid) {
    output.writeLong(uuid.getMostSignificantBits());
    output.writeLong(uuid.getLeastSignificantBits());
  }

  @Override
  public UUID read(Kryo kryo, Input input, Class<? extends UUID> aClass) {
    return new UUID(input.readLong(), input.readLong());
  }

}

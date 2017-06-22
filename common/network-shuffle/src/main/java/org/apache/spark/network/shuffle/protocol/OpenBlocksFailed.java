package org.apache.spark.network.shuffle.protocol;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

// Needed by ScalaDoc. See SPARK-7726
import static org.apache.spark.network.shuffle.protocol.BlockTransferMessage.Type;

/**
 * This message is responded from shuffle service when client failed to "open blocks" due to
 * some reason(e.g. the shuffle service is suffering from high memory cost).
 */
public class OpenBlocksFailed extends BlockTransferMessage {

  public final int reason;

  public OpenBlocksFailed(int reason) {
    this.reason = reason;
  }

  @Override
  protected Type type() { return Type.OPEN_BLOCKS_FAILED; }

  @Override
  public int hashCode() {
    return Objects.hashCode(reason);
  }

  public String toString() {
    String reasonStr = null;
    switch (reason) {
      case 1: reasonStr = "shuffle service is suffering high memory cost";
      default: reasonStr = "unknown";
    }
    return Objects.toStringHelper(this)
      .add("reason", reasonStr)
      .toString();
  }

  @Override
  public boolean equals(Object other) {
    if (other != null && other instanceof OpenBlocksFailed) {
      OpenBlocksFailed o = (OpenBlocksFailed) other;
      return Objects.equal(reason, o.reason);
    }
    return false;
  }

  @Override
  public int encodedLength() {
    return 4;
  }

  @Override
  public void encode(ByteBuf buf) {
    buf.writeInt(reason);
  }

  public static OpenBlocksFailed decode(ByteBuf buf) {
    int reason = buf.readInt();
    return new OpenBlocksFailed(reason);
  }
}

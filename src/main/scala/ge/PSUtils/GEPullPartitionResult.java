package ge.PSUtils;

import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;

public class GEPullPartitionResult extends PartitionGetResult {

    int start;
    int length;
    int dimension;
    float[] layers;
    ByteBuf buf;

    public GEPullPartitionResult(int start, int dimension, float[] layers) {
        this.start = start;
        this.layers = layers;
        this.dimension = dimension;
    }

    public GEPullPartitionResult() {}

    public void serialize(ByteBuf buf) {
        buf.writeInt(start);
        buf.writeInt(dimension);
        buf.writeInt(layers.length);
        for (int a = 0; a < layers.length; a ++) buf.writeFloat(layers[a]);
    }

    public void deserialize(ByteBuf buf) {
        start = buf.readInt();
        dimension = buf.readInt();
        length = buf.readInt();
        this.buf = buf.duplicate();
        this.buf.retain();
    }

    public void merge(float[] results) {
        int offset = start * dimension;
        for (int a = 0; a < length; a ++)
            results[a + offset] = buf.readFloat();
    }

    public void clear() {
        this.buf.release();
    }

    public int bufferLen() {
        return 4 + 4 + layers.length * 4;
    }
}

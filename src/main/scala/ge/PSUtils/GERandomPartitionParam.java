package ge.PSUtils;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import io.netty.buffer.ByteBuf;

public class GERandomPartitionParam extends PartitionUpdateParam {

    int dimension;

    public GERandomPartitionParam(int matrixId, PartitionKey partKey,
                                   int dimension) {
        super(matrixId, partKey);
        this.dimension = dimension;
    }

    public GERandomPartitionParam() { }

    @Override
    public void serialize(ByteBuf buf) {
        super.serialize(buf);
        buf.writeInt(dimension);
    }

    @Override
    public void deserialize(ByteBuf buf) {
        super.deserialize(buf);
        dimension = buf.readInt();
    }
}
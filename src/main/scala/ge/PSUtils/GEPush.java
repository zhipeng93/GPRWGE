package ge.PSUtils;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.math2.storage.IntFloatDenseVectorStorage;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.ps.storage.matrix.ServerPartition;

public class GEPush extends UpdateFunc {

    public GEPush(UpdateParam param) {
        super(param);
    }

    public GEPush() { super(null);}

    @Override
    public void partitionUpdate(PartitionUpdateParam partParam) {
        if (partParam instanceof GEPushPartitionParam) {
            GEPushPartitionParam param = (GEPushPartitionParam) partParam;
            try {
                update(psContext.getMatrixStorageManager().getPart(param.getPartKey()), param);
            } finally {
                param.clear();
            }
        }
    }

    private void update(ServerPartition partition,
                        GEPushPartitionParam param) {
        PartitionKey pkey = param.getPartKey();
        int totalRows = pkey.getEndRow() - pkey.getStartRow();
        int startRow  = pkey.getStartRow();
        float[][] rows = new float[totalRows][];
        int numNodePerRow = param.numNodePerRow;
        int startNode = startRow * numNodePerRow;
        int dimension = param.dimension;

        for (int row = startRow; row < startRow + totalRows; row ++)
            rows[row - startRow] = ((IntFloatDenseVectorStorage) partition
                    .getRow(row).getSplit().getStorage())
                    .getValues();

        for (int i = 0; i < param.length; i++) {
            int node = param.buf.readInt();
            int rowId  = (node - startNode) / numNodePerRow;
            int offset  = (node % numNodePerRow) * dimension;
            float[] values = rows[rowId];
            for (int d = 0; d < dimension; d ++)
                values[offset + d] += param.buf.readFloat();
        }
    }
}

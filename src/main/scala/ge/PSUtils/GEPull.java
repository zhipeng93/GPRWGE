package ge.PSUtils;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.math2.storage.IntFloatDenseVectorStorage;
import com.tencent.angel.ml.matrix.psf.get.base.*;

import java.util.List;

public class GEPull extends GetFunc {

    public GEPull(GetParam param) {
        super(param);
    }

    public GEPull() { super(null);}

    @Override
    public PartitionGetResult partitionGet(PartitionGetParam partParam) {

        if (partParam instanceof GEPullPartitionParam) {
            GEPullPartitionParam param = (GEPullPartitionParam) partParam;
            int[] indices = param.indices;
            int numNodePerRow = param.numNodePerRow;

            PartitionKey pkey = param.getPartKey();
            int startRow = pkey.getStartRow();
            int startNode = startRow * numNodePerRow;
            int dimension = param.dimension;

            float[] result = new float[indices.length * dimension];
            int idx = 0;

            int totalRows = pkey.getEndRow() - startRow;
            float[][] rows = new float[totalRows][];

            for (int row = startRow; row < startRow + totalRows; row ++)
                rows[row - startRow] = ((IntFloatDenseVectorStorage) psContext.getMatrixStorageManager()
                        .getRow(pkey, row).getSplit().getStorage())
                        .getValues();

            for (int a = 0; a < indices.length; a ++) {
                int node = indices[a];
                int rowId  = (node - startNode) / numNodePerRow;
                int offset  = (node % numNodePerRow) * dimension;
                float[] layer = rows[rowId];
                for (int b = 0; b < dimension; b ++) result[idx++] = layer[offset + b];
            }

            return new GEPullPartitionResult(param.start,
                    dimension,
                    result);
        }

        return null;
    }

    @Override
    public GetResult merge(List<PartitionGetResult> partResults) {

        if (this.param instanceof GEPullParam) {
            GEPullParam param = (GEPullParam) this.param;
            int[] indices = param.indices;
            int dimension = param.dimension;
            float[] layers = new float[indices.length * dimension];

            int total = 0;
            for (PartitionGetResult r: partResults) {
                total += ((GEPullPartitionResult)r).length;
            }
            assert total == (indices.length * dimension);

            for (PartitionGetResult r: partResults) {
                GEPullPartitionResult result = (GEPullPartitionResult) r;

                try {
                    result.merge(layers);
                } finally {
                    result.clear();
                }
            }
            return new GEPullResult(indices, layers);
        }
        return null;
    }
}

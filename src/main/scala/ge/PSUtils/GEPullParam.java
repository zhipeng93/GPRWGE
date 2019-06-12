package ge.PSUtils;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GEPullParam extends GetParam {

    int[] indices;
    int numNodePerRow;
    int dimension;

    public GEPullParam(int matrixId, int[] indices, int numNodePerRow, int dimension) {
        super(matrixId);
        this.indices = indices;
        this.numNodePerRow = numNodePerRow;
        this.dimension = dimension;
    }

    @Override
    public List<PartitionGetParam> split() {

        Arrays.sort(indices);
        List<PartitionKey> pkeys = PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);
        List<PartitionGetParam> params = new ArrayList<PartitionGetParam>();
        int start = 0, end = 0;
        for (PartitionKey pkey : pkeys) {
            int startRow = pkey.getStartRow();
            int endRow   = pkey.getEndRow();
            int startNode = startRow * numNodePerRow;
            int endNode   = endRow * numNodePerRow;

            if (start < indices.length && indices[start] >= startNode) {
                while (end < indices.length && indices[end] < endNode)
                    end++;

                if (end > start) {
                    params.add(new GEPullPartitionParam(matrixId,
                            pkey,
                            indices,
                            numNodePerRow,
                            start,
                            end - start,
                            dimension));
                }
                start = end;
            }
        }


        return params;
    }
}

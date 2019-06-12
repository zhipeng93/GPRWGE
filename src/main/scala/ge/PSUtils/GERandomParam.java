package ge.PSUtils;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.psagent.PSAgentContext;

import java.util.ArrayList;
import java.util.List;

public class GERandomParam extends UpdateParam {

    int dimension;

    public GERandomParam(int matrixId, int dimension) {
        super(matrixId);
        this.dimension = dimension;
    }

    @Override
    public List<PartitionUpdateParam> split() {
        List<PartitionUpdateParam> params = new ArrayList<PartitionUpdateParam>();
        List<PartitionKey> pkeys = PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);
        for (PartitionKey pkey: pkeys) {
            params.add(new GERandomPartitionParam(matrixId,
                    pkey,
                    dimension));
        }
        return params;
    }
}

package ge.PSUtils;

import com.tencent.angel.ml.matrix.psf.get.base.GetResult;

public class GEPullResult extends GetResult {
    public int[] indices;
    public float[] layers;

    public GEPullResult(int[] indices, float[] layers) {
        this.indices = indices;
        this.layers = layers;
    }

}

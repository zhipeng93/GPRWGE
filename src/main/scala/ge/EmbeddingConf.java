package ge;

public class EmbeddingConf {
  // possible values of each option is included in []

  // path of training data
  public static final String INPUTPATH = "INPUT"; // [inputpath]
  // path of output, i.e., the embedding vectors
  public static final String OUTPUTPATH = "OUTPUT"; // [outputpath]
  // the dimension of embedding vectors
  public static final String EMBEDDINGDIM = "DIMENSION"; // [100,]
  // number of negative samples
  public static final String NEGATIVESAMPLENUM = "NEGATIVE"; // [5,]
  // window size for word2vec
  public static final String WINDOWSIZE = "WINDOW"; // [5,]
  // number of epochs to train
  public static final String NUMEPOCH = "EPOCH"; // [10,]
  // learning rate
  public static final String STEPSIZE = "STEPSIZE"; // [0.1,]
  // batch size
  public static final String BATCHSIZE = "BATCHSIZE"; // [10,]
  // interval of checkpoint
  public static final String CHECKPOINTINTERVAL = "CHECKPOINT"; // [10,]
  // the stop probablity in rooted pagerank. The bigger, the longer the path is.
  public static final String STOPRATE = "STOPRATE"; //[0.5]
  public static final String SAMPLERNAME = "SAMPLER"; // [rootedPageRank,]

  public static final String FEATURES = "FEATURES";
  public static final String LABELS = "LABELS";

  public static final String PLATFORM = "PLATFORM";

  // ps parameters
  public static String NUMNODEPERROW = "NUMNODEPERROW";
  public static String NUMPARTS = "NUMPARTS";

}
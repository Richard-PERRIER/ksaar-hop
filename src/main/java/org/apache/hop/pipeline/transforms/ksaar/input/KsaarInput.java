package org.apache.hop.pipeline.transforms.ksaar.input;


import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.ksaar.Ksaar;
import org.json.JSONArray;
import org.json.JSONObject;

public class KsaarInput extends BaseTransform<KsaarInputMeta, KsaarInputData> {

  private static final Class<?> PKG = KsaarInputMeta.class; // For Translator

  public KsaarInput(
      TransformMeta transformMeta,
      KsaarInputMeta meta,
      KsaarInputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {
    boolean firstRow = true;

    String token = meta.getToken();
    JSONArray records = Ksaar.getRecords(token, meta.getWorkflowId());

    for (int i = 0; i < records.length(); i++) {
      JSONObject record = records.getJSONObject(i);

      if (firstRow) {
        for (String key : record.keySet()) {
          data.outputRowMeta.addValueMeta(new ValueMetaString(key));
        }

        firstRow = false;
      }

      Object[] outputRow = RowDataUtil.allocateRowData(data.outputRowMeta.size());

      int index = 0;
      for (String key : record.keySet()) {
        outputRow[index] = record.get(key);
        index++;
      }

      putRow(data.outputRowMeta, outputRow);
    }

    setOutputDone();
    return false;
  }

  @Override
  public boolean init() {
    super.init();

    if (data.outputRowMeta == null) {
      data.outputRowMeta = new RowMeta();
    }

    return true;
  }

  @Override
  public void dispose() {
    super.dispose();
  }
}

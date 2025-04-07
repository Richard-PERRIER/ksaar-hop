package org.apache.hop.pipeline.transforms.ksaar.input;


import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    List<String> searchKsaarFieldsNames = meta.getKsaarFields();
    List<String> searchStreamFieldsNames = meta.getStreamFields();

    Map<String, Integer> mappingFields = new HashMap<String, Integer>();

    JSONArray records = Ksaar.getRecords(token, meta.getWorkflowId());

    for (int i = 0; i < records.length(); i++) {
      JSONObject record = records.getJSONObject(i);

      if (firstRow) {
        for (int j = 0; j < searchKsaarFieldsNames.size(); j++) {
          data.outputRowMeta.addValueMeta(new ValueMetaString(searchStreamFieldsNames.get(j)));
          mappingFields.put(searchKsaarFieldsNames.get(j), j);
        }

        firstRow = false;
      }

      Object[] outputRow = RowDataUtil.allocateRowData(data.outputRowMeta.size());

      for (String key : record.keySet()) {
        if (mappingFields.containsKey(key)) {
          outputRow[mappingFields.get(key)] = record.get(key);
        }
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

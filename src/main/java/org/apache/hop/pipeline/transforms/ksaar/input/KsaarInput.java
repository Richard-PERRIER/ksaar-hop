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
    // Vérifier si les métadonnées sont correctement initialisées
    if (data.outputRowMeta == null) {
      throw new HopException("Les métadonnées de sortie ne sont pas initialisées !");
    }

    boolean firstRow = true;

    String token = meta.getTokenField();

    String applicationId = Ksaar.getApplication(token, meta.getApplicationField());
    String workflowId = Ksaar.getWorkflow(token, applicationId, meta.getWorkflowField());
    JSONArray records = Ksaar.getRecords(token, workflowId);

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

    setOutputDone(); // Indique que le processus est terminé
    return false; // Ne continue pas à traiter d'autres lignes
  }

  @Override
  public boolean init() {
    super.init();

    // Initialiser les métadonnées de sortie avec les colonnes appropriées
    if (data.outputRowMeta == null) {
      data.outputRowMeta = new RowMeta(); // Créer un nouveau RowMeta
    }

    return true;
  }

  @Override
  public void dispose() {
    super.dispose();
  }
}

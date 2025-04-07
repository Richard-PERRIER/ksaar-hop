/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.ksaar.output;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.ksaar.Ksaar;
import org.json.JSONArray;
import org.json.JSONObject;

public class KsaarOutput extends BaseTransform<KsaarOutputMeta, KsaarOutputData> {

  private static final Class<?> PKG = KsaarOutputMeta.class; // For Translator

  public KsaarOutput(
      TransformMeta transformMeta,
      KsaarOutputMeta meta,
      KsaarOutputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {

    List<IRowMeta> lines = new ArrayList<IRowMeta>();
    List<Object[]> rows = new ArrayList<Object[]>();

    while (true) {
      Object[] r = getRow(); // get row, set busy!
      if (r == null) break;
      lines.add(getInputRowMeta().clone());
      rows.add(r);
    }

    if (lines.size() == 0) {
      setOutputDone();
      return false;
    }

    String token = meta.getToken();
    String bulkType = meta.getBulkTypeField();

    JSONObject jsonRequest = new JSONObject();
    JSONArray records = new JSONArray();

    String[] streamFieldsNames = lines.get(0).getFieldNames();

    Map<String, Integer> mappingFields = new HashMap<String, Integer>();
    int idCol = -1;

    if (!bulkType.equals("DELETE")) {

      List<String> searchStreamFieldsNames = meta.getStreamFields();

      JSONArray ksaarFieldsNames = Ksaar.getFields(token, meta.getWorkflowId());
      List<String> searchKsaarFieldsNames = meta.getKsaarFields();

      for (int i = 0; i < ksaarFieldsNames.length(); i++) {
        JSONObject ksaarField = ksaarFieldsNames.getJSONObject(i);
        String ksaarName = ksaarField.getString("name");

        if (searchKsaarFieldsNames.contains(ksaarName)) {
          String searchStreamName = searchStreamFieldsNames.get(i);

          for (int j = 0; j < streamFieldsNames.length; j++) {
            String streamName = streamFieldsNames[j];

            if (searchStreamName.equals(streamName)) {
              mappingFields.put(ksaarField.getString("id"), j);
              break;
            }
          }
        }
      }
    }

    if (!bulkType.equals("CREATE")) {
      for (int i = 0; i < streamFieldsNames.length; i++) {
        String streamName = streamFieldsNames[i];

        if (streamName.equals(meta.getIdField())) {
          idCol = i;
          break;
        }
      }

      if (idCol == -1) throw new HopException("Error to find id column");
    }

    for (Object[] row : rows) {

      if (bulkType.equals("DELETE")) {
        records.put(row[idCol]);
      } else {
        JSONObject record = new JSONObject();

        if (bulkType.equals("CREATE")) record.put("email", meta.getEmailField());
        if (bulkType.equals("UPDATE")) record.put("id", row[idCol]);

        JSONObject form = new JSONObject();
        for (Entry<String, Integer> field : mappingFields.entrySet()) {
          form.put(field.getKey(), row[field.getValue()]);
        }
        record.put("form", form);

        records.put(record);
      }
    }

    if (bulkType.equals("CREATE")) {
      jsonRequest.put("workflowId", meta.getWorkflowId());
      jsonRequest.put("records", records);
      System.out.println(jsonRequest.toString());
      Ksaar.bulkCreate(token, jsonRequest.toString());
    }

    if (bulkType.equals("UPDATE")) {
      jsonRequest.put("applicationId", meta.getApplicationId());
      jsonRequest.put("records", records);
      System.out.println(jsonRequest.toString());
      Ksaar.bulkUpdate(token, jsonRequest.toString());
    }

    if (bulkType.equals("DELETE")) {
      jsonRequest.put("applicationId", meta.getApplicationId());
      jsonRequest.put("recordIds", records);
      System.out.println(jsonRequest.toString());
      Ksaar.bulkDelete(token, jsonRequest.toString());
    }

    setOutputDone();
    return true;
  }

  @Override
  public boolean init() {
    super.init();

    return true;
  }

  @Override
  public void dispose() {
    super.dispose();
  }
}

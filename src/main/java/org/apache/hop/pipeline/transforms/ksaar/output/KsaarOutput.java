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

    String token = meta.getTokenField();
    String bulkType = meta.getBulkTypeField();

    String applicationId = Ksaar.getApplication(token, meta.getApplicationField());
    String workflowId = Ksaar.getWorkflow(token, applicationId, meta.getWorkflowField());

    String[] fieldsNames = lines.get(0).getFieldNames();
    List<String> fieldsSearch = meta.getFields();
    JSONArray fieldsKsaar = Ksaar.getFields(token, workflowId);

    String idField = meta.getIdField();

    Map<String, Integer> fieldsMapping = new HashMap<String, Integer>();
    int idRow = -1;

    for (String fieldSearch : fieldsSearch) {
      System.out.println("Search: " + fieldSearch);
      int row = -1;
      String uuid = "";

      for (int i = 0; i < fieldsNames.length; i++) {
        System.out.println("fieldName: " + fieldsNames[i]);
        if (fieldSearch.equals(fieldsNames[i])) {
          row = i;
          break;
        }
      }

      for (int i = 0; i < fieldsKsaar.length(); i++) {
        JSONObject fieldKsaar = fieldsKsaar.getJSONObject(i);
        System.out.println("fieldKsaar: " + fieldKsaar.getString("name"));

        if (fieldSearch.equals(fieldKsaar.getString("name"))) {
          uuid = fieldKsaar.getString("id");
          break;
        }
      }

      if (row == -1 || uuid == "") continue;

      System.out.println(uuid + ": " + row);
      fieldsMapping.put(uuid, row);
    }

    if (!idField.isEmpty()) {
      for (int i = 0; i < fieldsNames.length; i++) {
        if (idField.equals(fieldsNames[i])) {
          idRow = i;
          break;
        }
      }
    }

    JSONObject jsonRequest = new JSONObject();

    JSONArray records = new JSONArray();
    for (Object[] row : rows) {

      if (bulkType.equals("DELETE")) {
        records.put(row[idRow]);
      } else {

        JSONObject record = new JSONObject();

        if (bulkType.equals("CREATE")) {
          record.put("email", meta.getEmailField());
          jsonRequest.put("workflowId", workflowId);
        }

        if (bulkType.equals("UPDATE")) {
          record.put("id", row[idRow]);
          jsonRequest.put("applicationId", applicationId);
        }

        JSONObject form = new JSONObject();
        for (Entry<String, Integer> field : fieldsMapping.entrySet()) {
          form.put(field.getKey(), row[field.getValue()]);
        }
        record.put("form", form);

        records.put(record);
      }
    }

    if (bulkType.equals("DELETE")) {
      jsonRequest.put("applicationId", applicationId);
      jsonRequest.put("recordIds", records);
    } else {
      jsonRequest.put("records", records);
    }

    if (bulkType.equals("CREATE")) Ksaar.bulkCreate(token, jsonRequest.toString());
    if (bulkType.equals("UPDATE")) Ksaar.bulkUpdate(token, jsonRequest.toString());
    if (bulkType.equals("DELETE")) Ksaar.bulkDelete(token, jsonRequest.toString());

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

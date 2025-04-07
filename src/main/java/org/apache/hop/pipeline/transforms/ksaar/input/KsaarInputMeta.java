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

package org.apache.hop.pipeline.transforms.ksaar.input;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

@Transform(
    id = "KsaarInput",
    image = "ksaar.svg",
    name = "i18n::BaseTransform.TypeLongDesc.KsaarInput",
    description = "i18n::BaseTransform.TypeTooltipDesc.KsaarInput",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input")
public class KsaarInputMeta extends BaseTransformMeta<KsaarInput, KsaarInputData> {

  private static final Class<?> PKG = KsaarInputMeta.class; // For Translator

  public Map<String, String> workflows;
  public Map<String, String> fields;

  private String tokenField;
  private String workflowField;

  private String token;
  private String applicationId;
  private String workflowId;

  private List<String> ksaarFields;
  private List<String> streamFields;

  public KsaarInputMeta() {
    super(); // allocate BaseTransformMeta
  }

  public Map<String, String> getWorkflows() {
    return workflows;
  }

  public void setWorkflows(Map<String, String> workflows) {
    this.workflows = workflows;
  }

  public String getTokenField() {
    return tokenField;
  }

  public void setTokenField(String tokenField) {
    this.tokenField = tokenField;
  }

  public String getWorkflowField() {
    return workflowField;
  }

  public void setWorkflowField(String workflowField) {
    this.workflowField = workflowField;
  }

  public String getToken() {
    return token;
  }

  public void setToken(String token) {
    this.token = token;
  }

  public String getApplicationId() {
    return applicationId;
  }

  public void setApplicationId(String applicationId) {
    this.applicationId = applicationId;
  }

  public String getWorkflowId() {
    return workflowId;
  }

  public void setWorkflowId(String workflowId) {
    this.workflowId = workflowId;
  }

  public List<String> getKsaarFields() {
    return ksaarFields;
  }

  public void setKsaarFields(List<String> ksaarFields) {
    this.ksaarFields = ksaarFields;
  }

  public List<String> getStreamFields() {
    return streamFields;
  }

  public void setStreamFields(List<String> streamFields) {
    this.streamFields = streamFields;
  }

  public Map<String, String> getFields() {
    return fields;
  }

  public void setFields(Map<String, String> fields) {
    this.fields = fields;
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode);
  }

  @Override
  public void setDefault() {}

  private void readData(Node transformNode) {
    tokenField = XmlHandler.getTagValue(transformNode, "tokenField");
    workflowField = XmlHandler.getTagValue(transformNode, "workflowField");

    token = XmlHandler.getTagValue(transformNode, "token");
    applicationId = XmlHandler.getTagValue(transformNode, "applicationId");
    workflowId = XmlHandler.getTagValue(transformNode, "workflowId");

    // ----------------------------------------------\\

    ksaarFields = new ArrayList<String>();
    NodeList fieldNodes = transformNode.getChildNodes();
    for (int i = 0; i < fieldNodes.getLength(); i++) {
      Node fieldNode = fieldNodes.item(i);

      if (fieldNode.getNodeType() == Node.ELEMENT_NODE
          && fieldNode.getNodeName().equals("ksaarField")) {
        ksaarFields.add(fieldNode.getTextContent());
      }
    }

    streamFields = new ArrayList<String>();
    NodeList streamNodes = transformNode.getChildNodes();
    for (int i = 0; i < streamNodes.getLength(); i++) {
      Node streamNode = streamNodes.item(i);

      if (streamNode.getNodeType() == Node.ELEMENT_NODE
          && streamNode.getNodeName().equals("streamField")) {
        streamFields.add(streamNode.getTextContent());
      }
    }
  }

  @Override
  public void getFields(
      IRowMeta r,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    if (r == null) {
      r = new RowMeta(); // give back values
    }

    // No values are added to the row in this type of transform
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder();
    retval.append("    " + XmlHandler.addTagValue("tokenField", tokenField));
    retval.append("    " + XmlHandler.addTagValue("workflowField", workflowField));

    retval.append("    " + XmlHandler.addTagValue("token", token));
    retval.append("    " + XmlHandler.addTagValue("applicationId", applicationId));
    retval.append("    " + XmlHandler.addTagValue("workflowId", workflowId));

    // ----------------------------------------------\\

    if (ksaarFields != null && !ksaarFields.isEmpty()) {
      for (String field : ksaarFields) {
        retval.append("    " + XmlHandler.addTagValue("ksaarField", field));
      }
    }

    if (streamFields != null && !streamFields.isEmpty()) {
      for (String field : streamFields) {
        retval.append("    " + XmlHandler.addTagValue("streamField", field));
      }
    }

    return retval.toString();
  }

  @Override
  public void resetTransformIoMeta() {}
}

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


import java.util.List;
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

@Transform(
    id = "KsaarOutput",
    image = "ksaaroutput.svg",
    name = "i18n::BaseTransform.TypeLongDesc.KsaarOutput",
    description = "i18n::BaseTransform.TypeTooltipDesc.KsaarOutput",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Output")
public class KsaarOutputMeta extends BaseTransformMeta<KsaarOutput, KsaarOutputData> {
  private static final Class<?> PKG = KsaarOutputMeta.class; // For Translator

  private String bulkTypeField;
  private String tokenField;
  private String applicationField;
  private String workflowField;
  private String emailField;
  private String idField;

  private List<String> fields;

  public KsaarOutputMeta() {
    super(); // allocate BaseTransformMeta
  }

  public String getBulkTypeField() {
    return bulkTypeField;
  }

  public void setBulkTypeField(String bulkTypeField) {
    this.bulkTypeField = bulkTypeField;
  }

  public String getTokenField() {
    return tokenField;
  }

  public void setTokenField(String tokenField) {
    this.tokenField = tokenField;
  }

  public String getApplicationField() {
    return applicationField;
  }

  public void setApplicationField(String applicationField) {
    this.applicationField = applicationField;
  }

  public String getWorkflowField() {
    return workflowField;
  }

  public void setWorkflowField(String workflowField) {
    this.workflowField = workflowField;
  }

  public String getEmailField() {
    return emailField;
  }

  public void setEmailField(String emailField) {
    this.emailField = emailField;
  }

  public String getIdField() {
    return idField;
  }

  public void setIdField(String idField) {
    this.idField = idField;
  }

  public List<String> getFields() {
    return fields;
  }

  public void setFields(List<String> fields) {
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
    bulkTypeField = XmlHandler.getTagValue(transformNode, "bulkTypeField");
    tokenField = XmlHandler.getTagValue(transformNode, "tokenField");
    applicationField = XmlHandler.getTagValue(transformNode, "applicationField");
    workflowField = XmlHandler.getTagValue(transformNode, "workflowField");
    emailField = XmlHandler.getTagValue(transformNode, "emailField");
    idField = XmlHandler.getTagValue(transformNode, "idField");
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
    retval.append("    " + XmlHandler.addTagValue("bulkTypeField", bulkTypeField));
    retval.append("    " + XmlHandler.addTagValue("tokenField", tokenField));
    retval.append("    " + XmlHandler.addTagValue("applicationField", applicationField));
    retval.append("    " + XmlHandler.addTagValue("workflowField", workflowField));
    retval.append("    " + XmlHandler.addTagValue("emailField", emailField));
    retval.append("    " + XmlHandler.addTagValue("idField", idField));
    return retval.toString();
  }

  @Override
  public void resetTransformIoMeta() {}
}

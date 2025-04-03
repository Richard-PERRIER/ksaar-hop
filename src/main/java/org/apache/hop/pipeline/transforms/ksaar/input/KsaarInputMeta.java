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
    id = "KsaarInput",
    image = "ksaarinput.svg",
    name = "i18n::BaseTransform.TypeLongDesc.KsaarInput",
    description = "i18n::BaseTransform.TypeTooltipDesc.KsaarInput",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input")
public class KsaarInputMeta extends BaseTransformMeta<KsaarInput, KsaarInputData> {

  private static final Class<?> PKG = KsaarInputMeta.class; // For Translator

  private String tokenField;
  private String applicationField;
  private String workflowField;

  public KsaarInputMeta() {
    super(); // allocate BaseTransformMeta
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

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode);
  }

  @Override
  public void setDefault() {}

  private void readData(Node transformNode) {
    tokenField = XmlHandler.getTagValue(transformNode, "tokenField");
    applicationField = XmlHandler.getTagValue(transformNode, "applicationField");
    workflowField = XmlHandler.getTagValue(transformNode, "workflowField");
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
    retval.append("    " + XmlHandler.addTagValue("applicationField", applicationField));
    retval.append("    " + XmlHandler.addTagValue("workflowField", workflowField));
    return retval.toString();
  }

  @Override
  public void resetTransformIoMeta() {}
}

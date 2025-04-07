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


import java.util.*;
import java.util.Map.Entry;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transforms.ksaar.Ksaar;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;
import org.json.JSONArray;
import org.json.JSONObject;

public class KsaarInputDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = KsaarInputMeta.class; // For Translator

  private static final int MARGIN_SIZE = 15;
  private static final int ELEMENT_SPACING = Const.MARGIN;

  private final KsaarInputMeta meta;

  private Text wTransformNameField;

  private CCombo wWorkflowField;

  private TextVar wTokenField;

  private MessageBox dialog;

  private String token;
  private String applicationId;
  private boolean changed;

  public KsaarInputDialog(
      Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname) {
    super(parent, variables, (BaseTransformMeta) in, pipelineMeta, sname);
    meta = (KsaarInputMeta) in;
  }

  @Override
  public String open() {
    // Set up window
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
    props.setLook(shell);
    setShellImage(shell, meta);
    int middle = props.getMiddlePct();

    // Listeners
    ModifyListener lsMod = e -> meta.setChanged();
    changed = meta.hasChanged();

    // 15 pixel margins
    FormLayout formLayout = new FormLayout();
    formLayout.marginLeft = MARGIN_SIZE;
    formLayout.marginHeight = MARGIN_SIZE;
    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "KsaarInputDialog.Shell.Title"));

    // Build a scrolling composite and a composite for holding all content
    ScrolledComposite scrolledComposite = new ScrolledComposite(shell, SWT.V_SCROLL);
    Composite contentComposite = new Composite(scrolledComposite, SWT.NONE);
    FormLayout contentLayout = new FormLayout();
    contentLayout.marginRight = MARGIN_SIZE;
    contentComposite.setLayout(contentLayout);
    FormData compositeLayoutData = new FormDataBuilder().fullSize().result();
    contentComposite.setLayoutData(compositeLayoutData);
    props.setLook(contentComposite);

    // --------------------- TOP --------------------- \\

    // transform name label and text field
    // Transform Name.
    Label wTransformNameLabel = new Label(contentComposite, SWT.RIGHT);
    wTransformNameLabel.setText(
        BaseMessages.getString(PKG, "KsaarInputDialog.TransformName.Label"));
    props.setLook(wTransformNameLabel);
    FormData fdTransformNameLabel =
        new FormDataBuilder().left().top().right(middle, -ELEMENT_SPACING).result();
    wTransformNameLabel.setLayoutData(fdTransformNameLabel);

    wTransformNameField = new Text(contentComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformNameField.setText(transformName);
    props.setLook(wTransformNameField);
    wTransformNameField.addModifyListener(lsMod);
    FormData fdTransformName = new FormDataBuilder().left(middle, 0).top().right(100, 0).result();
    wTransformNameField.setLayoutData(fdTransformName);

    // Spacer between entry info and content
    Label topSpacer = new Label(contentComposite, SWT.HORIZONTAL | SWT.SEPARATOR);
    FormData fdSpacer =
        new FormDataBuilder().fullWidth().top(wTransformNameField, MARGIN_SIZE).result();
    topSpacer.setLayoutData(fdSpacer);

    // --------------------- CONTENT --------------------- \\

    // Create a label for the "Token" field
    Label wTokenLabel = new Label(contentComposite, SWT.RIGHT);
    wTokenLabel.setText(BaseMessages.getString(PKG, "KsaarInputDialog.TokenName.Label"));
    props.setLook(wTokenLabel);
    FormData fdTokenLabel =
        new FormDataBuilder()
            .left()
            .top(topSpacer, ELEMENT_SPACING)
            .right(middle, -ELEMENT_SPACING)
            .result();
    wTokenLabel.setLayoutData(fdTokenLabel);

    // Create the TextVar widget for "Workflow name"
    wTokenField = new TextVar(variables, contentComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTokenField.setText(""); // Default text can be set here, or leave it blank
    props.setLook(wTokenField);
    FormData fdToken =
        new FormDataBuilder()
            .left(middle, 0)
            .top(topSpacer, ELEMENT_SPACING)
            .right(100, 0)
            .result();
    wTokenField.setLayoutData(fdToken);

    // ------------------------ \\

    // Create a label for the "Workflow" field
    Label wWorkflowLabel = new Label(contentComposite, SWT.RIGHT);
    wWorkflowLabel.setText(BaseMessages.getString(PKG, "KsaarInputDialog.WorkflowName.Label"));
    props.setLook(wWorkflowLabel);
    FormData fdWorkflowLabel =
        new FormDataBuilder()
            .left()
            .top(wTokenField, ELEMENT_SPACING)
            .right(middle, -ELEMENT_SPACING)
            .result();
    wWorkflowLabel.setLayoutData(fdWorkflowLabel);

    // Create the TextVar widget for "Workflow name"
    wWorkflowField = new CCombo(contentComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wWorkflowField.setText(""); // Default text can be set here, or leave it blank
    props.setLook(wWorkflowField);
    FormData fdWorkflow =
        new FormDataBuilder()
            .left(middle, 0)
            .top(wTokenField, ELEMENT_SPACING)
            .right(100, 0)
            .result();
    wWorkflowField.setLayoutData(fdWorkflow);

    // --------------------- BOTTOM --------------------- \\

    // Cancel, action and OK buttons for the bottom of the window.
    // Footer Buttons
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    FormData fdCancel = new FormDataBuilder().bottom().left(50, -10).result();
    wCancel.setLayoutData(fdCancel);

    Button wOK = new Button(shell, SWT.PUSH);
    wOK.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    FormData fdOk = new FormDataBuilder().bottom().left(50, -80).result();
    wOK.setLayoutData(fdOk);

    // Space between bottom buttons and group content.
    Label bottomSpacer = new Label(shell, SWT.HORIZONTAL | SWT.SEPARATOR);
    FormData fdhSpacer =
        new FormDataBuilder()
            .left()
            .right(100, -MARGIN_SIZE)
            .bottom(wCancel, -MARGIN_SIZE)
            .result();
    bottomSpacer.setLayoutData(fdhSpacer);

    // Add everything to the scrolling composite
    scrolledComposite.setContent(contentComposite);
    scrolledComposite.setExpandVertical(true);
    scrolledComposite.setExpandHorizontal(true);
    scrolledComposite.setMinSize(contentComposite.computeSize(SWT.DEFAULT, SWT.DEFAULT));

    scrolledComposite.setLayout(new FormLayout());
    FormData fdScrolledComposite =
        new FormDataBuilder().fullWidth().top().bottom(bottomSpacer, -MARGIN_SIZE).result();
    scrolledComposite.setLayoutData(fdScrolledComposite);
    props.setLook(scrolledComposite);

    // Listeners
    Listener lsOK = e -> ok();

    wOK.addListener(SWT.Selection, lsOK);
    wCancel.addListener(SWT.Selection, e -> cancel());

    getData();

    wTokenField.addFocusListener(
        new FocusListener() {
          @Override
          public void focusGained(FocusEvent e) {}

          @Override
          public void focusLost(FocusEvent e) {
            if (wTokenField.getText().length() == 0) return;

            token = variables.resolve(wTokenField.getText());
            updateApplications();
            updateWorkflows();
          }
        });

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void updateApplications() {
    try {
      JSONArray applications = Ksaar.getApplication(token);

      if (applications == null) {
        dialog.setText(BaseMessages.getString(PKG, "Ksaar.Message.Title"));
        dialog.setMessage(BaseMessages.getString(PKG, "Ksaar.Error.ApplicationNotFound"));
        dialog.open();

        return;
      }

      JSONObject application = applications.getJSONObject(0);

      applicationId = application.getString("id");
    } catch (HopException e) {
      dialog.setText(BaseMessages.getString(PKG, "Ksaar.Message.Title"));
      dialog.setMessage(BaseMessages.getString(PKG, e.getMessage()));
      dialog.open();
    }
  }

  private void updateWorkflows() {
    wWorkflowField.removeAll();

    Map<String, String> workflows = meta.getWorkflows();

    if (workflows != null) {
      System.out.println("workflow meta");
      for (Entry<String, String> workflow : workflows.entrySet()) {
        wWorkflowField.add(workflow.getKey());
      }

      return;
    }

    try {
      JSONArray ksaarWorkflows = Ksaar.getWorkflows(token, applicationId);
      System.out.println("workflow request");
      if (ksaarWorkflows == null) {
        dialog.setText(BaseMessages.getString(PKG, "Ksaar.Message.Title"));
        dialog.setMessage(BaseMessages.getString(PKG, "Ksaar.Error.WorkflowsNotFound"));
        dialog.open();

        return;
      }

      workflows = new HashMap<String, String>();

      for (int i = 0; i < ksaarWorkflows.length(); i++) {
        JSONObject workflow = ksaarWorkflows.getJSONObject(i);
        wWorkflowField.add(workflow.getString("name"));
        workflows.put(workflow.getString("name"), workflow.getString("id"));
      }

      meta.setWorkflows(workflows);
    } catch (HopException e) {
      dialog.setText(BaseMessages.getString(PKG, "Ksaar.Message.Title"));
      dialog.setMessage(BaseMessages.getString(PKG, e.getMessage()));
      dialog.open();
    }
  }

  private void getData() {
    String tokenField = meta.getTokenField();
    if (tokenField != null) wTokenField.setText(tokenField);
    else return;
    this.token = meta.getToken();

    String applicationId = meta.getApplicationId();
    if (applicationId == null) return;
    this.applicationId = applicationId;

    updateWorkflows();

    String workflowField = meta.getWorkflowField();
    if (workflowField != null) {
      String[] workflowsItems = wWorkflowField.getItems();
      for (int i = 0; i < workflowsItems.length; i++) {
        if (workflowsItems[i].equals(workflowField)) {
          wWorkflowField.select(i);
          break;
        }
      }
    }
  }

  private void setMeta(KsaarInputMeta meta) {
    meta.setTokenField(wTokenField.getText());
    meta.setWorkflowField(wWorkflowField.getText());

    meta.setToken(token);
    meta.setApplicationId(applicationId);
    meta.setWorkflowId(meta.workflows.get(wWorkflowField.getText()));
  }

  private void cancel() {
    meta.setChanged(changed);
    dispose();
  }

  private void ok() {
    setMeta(meta);
    transformName = wTransformNameField.getText();
    dispose();
  }
}

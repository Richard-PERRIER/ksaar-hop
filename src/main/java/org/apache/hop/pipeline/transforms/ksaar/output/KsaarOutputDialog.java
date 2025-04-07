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


import java.util.*;
import java.util.List;
import java.util.Map.Entry;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transforms.ksaar.Ksaar;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
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

public class KsaarOutputDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = KsaarOutputMeta.class; // For Translator

  private static final int MARGIN_SIZE = 15;
  private static final int ELEMENT_SPACING = Const.MARGIN;

  private final KsaarOutputMeta meta;

  private Text wTransformNameField;

  private CCombo wBulkTypeField;
  private CCombo wEmailField;
  private CCombo wWorkflowField;

  private TextVar wTokenField;

  private CCombo wIdField;

  private TableView wFields;

  private ColumnInfo wKsaarField;
  private ColumnInfo wStreamField;

  private MessageBox dialog;

  private String token;
  private String applicationId;
  private boolean changed;

  public KsaarOutputDialog(
      Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname) {
    super(parent, variables, (BaseTransformMeta) in, pipelineMeta, sname);
    meta = (KsaarOutputMeta) in;
  }

  @Override
  public String open() {
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
    props.setLook(shell);
    setShellImage(shell, meta);
    int middle = props.getMiddlePct();

    ModifyListener lsMod = e -> meta.setChanged();
    changed = meta.hasChanged();

    dialog = new MessageBox(shell);

    FormLayout formLayout = new FormLayout();
    formLayout.marginLeft = MARGIN_SIZE;
    formLayout.marginHeight = MARGIN_SIZE;
    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "KsaarOutputDialog.Shell.Title"));

    ScrolledComposite scrolledComposite = new ScrolledComposite(shell, SWT.V_SCROLL);
    Composite contentComposite = new Composite(scrolledComposite, SWT.NONE);
    FormLayout contentLayout = new FormLayout();
    contentLayout.marginRight = MARGIN_SIZE;
    contentComposite.setLayout(contentLayout);
    FormData compositeLayoutData = new FormDataBuilder().fullSize().result();
    contentComposite.setLayoutData(compositeLayoutData);
    props.setLook(contentComposite);

    // --------------------- TOP --------------------- \\

    Label wTransformNameLabel = new Label(contentComposite, SWT.RIGHT);
    wTransformNameLabel.setText(
        BaseMessages.getString(PKG, "KsaarOutputDialog.TransformName.Label"));
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

    Label topSpacer = new Label(contentComposite, SWT.HORIZONTAL | SWT.SEPARATOR);
    FormData fdSpacer =
        new FormDataBuilder().fullWidth().top(wTransformNameField, MARGIN_SIZE).result();
    topSpacer.setLayoutData(fdSpacer);

    // --------------------- CONTENT --------------------- \\

    Label wTokenLabel = new Label(contentComposite, SWT.RIGHT);
    wTokenLabel.setText(BaseMessages.getString(PKG, "KsaarOutputDialog.TokenName.Label"));
    props.setLook(wTokenLabel);
    FormData fdTokenLabel =
        new FormDataBuilder()
            .left()
            .top(topSpacer, ELEMENT_SPACING)
            .right(middle, -ELEMENT_SPACING)
            .result();
    wTokenLabel.setLayoutData(fdTokenLabel);

    wTokenField = new TextVar(variables, contentComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTokenField.setText("");
    props.setLook(wTokenField);
    FormData fdToken =
        new FormDataBuilder()
            .left(middle, 0)
            .top(topSpacer, ELEMENT_SPACING)
            .right(100, 0)
            .result();
    wTokenField.setLayoutData(fdToken);

    // ------------------------ \\

    Label wWorkflowLabel = new Label(contentComposite, SWT.RIGHT);
    wWorkflowLabel.setText(BaseMessages.getString(PKG, "KsaarOutputDialog.WorkflowName.Label"));
    props.setLook(wWorkflowLabel);
    FormData fdWorkflowLabel =
        new FormDataBuilder()
            .left()
            .top(wTokenField, ELEMENT_SPACING)
            .right(middle, -ELEMENT_SPACING)
            .result();
    wWorkflowLabel.setLayoutData(fdWorkflowLabel);

    wWorkflowField = new CCombo(contentComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wWorkflowField.setText("");
    props.setLook(wWorkflowField);
    FormData fdWorkflow =
        new FormDataBuilder()
            .left(middle, 0)
            .top(wTokenField, ELEMENT_SPACING)
            .right(100, 0)
            .result();
    wWorkflowField.setLayoutData(fdWorkflow);

    // ------------------------ \\

    Label wBulkTypeLabel = new Label(contentComposite, SWT.RIGHT);
    wBulkTypeLabel.setText(BaseMessages.getString(PKG, "KsaarOutputDialog.BulkType.Label"));
    props.setLook(wBulkTypeLabel);
    FormData fdBulkTypeLabel =
        new FormDataBuilder()
            .left()
            .top(wWorkflowField, ELEMENT_SPACING)
            .right(middle, -ELEMENT_SPACING)
            .result();
    wBulkTypeLabel.setLayoutData(fdBulkTypeLabel);

    wBulkTypeField = new CCombo(contentComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wBulkTypeField.setText("");
    props.setLook(wBulkTypeField);
    FormData fdBulkType =
        new FormDataBuilder()
            .left(middle, 0)
            .top(wWorkflowField, ELEMENT_SPACING)
            .right(100, 0)
            .result();
    wBulkTypeField.setLayoutData(fdBulkType);
    wBulkTypeField.addModifyListener(e -> updateBulk(wBulkTypeField.getText()));

    // ------------------------ \\

    Label wEmailLabel = new Label(contentComposite, SWT.RIGHT);
    wEmailLabel.setText(BaseMessages.getString(PKG, "KsaarOutputDialog.Email.Label"));
    props.setLook(wEmailLabel);
    FormData fdEmailLabel =
        new FormDataBuilder()
            .left()
            .top(wBulkTypeField, ELEMENT_SPACING)
            .right(middle, -ELEMENT_SPACING)
            .result();
    wEmailLabel.setLayoutData(fdEmailLabel);

    wEmailField = new CCombo(contentComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wEmailField.setText("");
    props.setLook(wEmailField);
    FormData fdEmail =
        new FormDataBuilder()
            .left(middle, 0)
            .top(wBulkTypeField, ELEMENT_SPACING)
            .right(100, 0)
            .result();
    wEmailField.setLayoutData(fdEmail);

    // ------------------------ \\

    Label wIdLabel = new Label(contentComposite, SWT.RIGHT);
    wIdLabel.setText(BaseMessages.getString(PKG, "KsaarOutputDialog.Id.Label"));
    props.setLook(wIdLabel);
    FormData fdIdLabel =
        new FormDataBuilder()
            .left()
            .top(wEmailField, ELEMENT_SPACING)
            .right(middle, -ELEMENT_SPACING)
            .result();
    wIdLabel.setLayoutData(fdIdLabel);

    wIdField = new CCombo(contentComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wIdField.setText("");
    props.setLook(wIdField);
    FormData fdId =
        new FormDataBuilder()
            .left(middle, 0)
            .top(wEmailField, ELEMENT_SPACING)
            .right(100, 0)
            .result();
    wIdField.setLayoutData(fdId);

    // ------------------------ \\

    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = Const.FORM_MARGIN;
    fieldsLayout.marginHeight = Const.FORM_MARGIN;

    ColumnInfo[] colinf =
        new ColumnInfo[] {
          wKsaarField =
              new ColumnInfo(
                  BaseMessages.getString(PKG, "KsaarOutputDialog.KsaarColumn.Column"),
                  ColumnInfo.COLUMN_TYPE_CCOMBO,
                  new String[] {""},
                  false),
          wStreamField =
              new ColumnInfo(
                  BaseMessages.getString(PKG, "KsaarOutputDialog.StreamColumn.Column"),
                  ColumnInfo.COLUMN_TYPE_CCOMBO,
                  new String[] {""},
                  false)
        };

    wFields =
        new TableView(
            variables,
            contentComposite,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            colinf,
            meta.getKsaarFields() == null ? 1 : meta.getKsaarFields().size(),
            lsMod,
            props);
    FormData fdFields =
        new FormDataBuilder()
            .left(middle, 0)
            .top(wIdField, ELEMENT_SPACING)
            .right(100, 0)
            .fullWidth()
            .height(200)
            .result();
    wFields.setLayoutData(fdFields);
    props.setLook(wFields);

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

    wWorkflowField.addModifyListener(
        e -> {
          updateFields(wWorkflowField.getText());
        });

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void updateBulk(String bulkType) {
    switch (bulkType) {
      case "CREATE":
        wEmailField.setEnabled(true);
        wIdField.setEnabled(false);
        wFields.setVisible(true);
        break;

      case "UPDATE":
        wEmailField.setEnabled(false);
        wIdField.setEnabled(true);
        wFields.setVisible(true);
        break;
      case "DELETE":
        wEmailField.setEnabled(false);
        wIdField.setEnabled(true);
        wFields.setVisible(false);
        break;
    }
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

  private void updateUsers() {
    wEmailField.removeAll();

    List<String> users = meta.getUsers();

    if (users != null) {
      for (String user : users) {
        wEmailField.add(user);
      }

      return;
    }

    try {
      JSONArray ksaarUsers = Ksaar.getUsers(token, applicationId);

      users = new ArrayList<String>();

      for (int i = 0; i < ksaarUsers.length(); i++) {
        JSONObject user = ksaarUsers.getJSONObject(i);
        wEmailField.add(user.getString("email"));
        users.add(user.getString("email"));
      }

      meta.setUsers(users);
    } catch (HopException e) {
      e.printStackTrace();
    }
  }

  private void updateFields(String workflowName) {
    String[] fieldsValue = null;

    Map<String, String> fields = meta.getFields();

    String workflowField = meta.getWorkflowField();
    if (workflowField != null && wWorkflowField.getText().equals(workflowField)) {
      if (fields != null) {
        fieldsValue = new String[fields.size()];
        int i = 0;
        for (Entry<String, String> field : fields.entrySet()) {
          fieldsValue[i] = field.getKey();
          i++;
        }

        wKsaarField.setComboValues(fieldsValue);

        return;
      }
    }

    String workflowId = meta.workflows.get(workflowName);

    try {
      JSONArray ksaarFields = Ksaar.getFields(token, workflowId);

      fieldsValue = new String[ksaarFields.length()];
      fields = new HashMap<String, String>();

      for (int i = 0; i < ksaarFields.length(); i++) {
        JSONObject field = ksaarFields.getJSONObject(i);
        fieldsValue[i] = field.getString("name");
        fields.put(field.getString("name"), field.getString("id"));
      }

      wKsaarField.setComboValues(fieldsValue);
      meta.setFields(fields);
    } catch (HopException e) {
      e.printStackTrace();
    }
  }

  private void getData() {
    wBulkTypeField.setItems(meta.bulkTypeString);
    wBulkTypeField.select(0);

    String bulkType = meta.getBulkTypeField();
    if (bulkType != null)
      wBulkTypeField.select(Arrays.asList(meta.bulkTypeString).indexOf(bulkType));

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

    updateUsers();

    String emailField = meta.getEmailField();
    if (emailField != null) {
      String[] emailItems = wEmailField.getItems();
      for (int i = 0; i < emailItems.length; i++) {
        if (emailItems[i].equals(emailField)) {
          wEmailField.select(i);
          break;
        }
      }
    }

    try {
      IRowMeta row = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (row != null) {
        String[] fieldsName = row.getFieldNames();
        wIdField.setItems(fieldsName);
        wStreamField.setComboValues(fieldsName);
      }
    } catch (HopTransformException e) {
      System.out.println("Error to get previous transform fields.");
    }

    String idField = meta.getIdField();
    if (idField != null) wIdField.setText(idField);

    List<String> ksaarFields = meta.getKsaarFields();
    if (ksaarFields != null) {
      Table fieldsTable = wFields.table;

      for (int i = 0; i < ksaarFields.size(); i++) {
        TableItem item = fieldsTable.getItem(i);
        item.setText(1, ksaarFields.get(i));
      }

      fieldsTable.redraw();
    }

    List<String> streamFields = meta.getStreamFields();
    if (streamFields != null) {
      Table fieldsTable = wFields.table;

      for (int i = 0; i < streamFields.size(); i++) {
        TableItem item = fieldsTable.getItem(i);
        item.setText(2, streamFields.get(i));
      }

      fieldsTable.redraw();
    }

    updateFields(workflowField);
  }

  private void setMeta(KsaarOutputMeta meta) {
    meta.setBulkTypeField(wBulkTypeField.getText());
    meta.setTokenField(wTokenField.getText());
    meta.setWorkflowField(wWorkflowField.getText());

    meta.setToken(token);
    meta.setApplicationId(applicationId);
    meta.setWorkflowId(meta.workflows.get(wWorkflowField.getText()));

    meta.setEmailField(wEmailField.getText());
    meta.setIdField(wIdField.getText());

    Table fieldsTable = wFields.table;
    List<String> ksaarFields = new ArrayList<String>();
    List<String> streamFields = new ArrayList<String>();

    for (int i = 0; i < fieldsTable.getItemCount(); i++) {
      TableItem item = fieldsTable.getItem(i);
      ksaarFields.add(item.getText(1));
      streamFields.add(item.getText(2));
    }

    meta.setKsaarFields(ksaarFields);
    meta.setStreamFields(streamFields);
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

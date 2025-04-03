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
import org.apache.hop.core.Const;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.stream.IStream;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

public class KsaarOutputDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = KsaarOutputMeta.class; // For Translator

  private static final int MARGIN_SIZE = 15;
  private static final int ELEMENT_SPACING = Const.MARGIN;

  private final KsaarOutputMeta meta;

  private Text wTransformNameField;

  private CCombo wBulkTypeField;

  private TextVar wEmailField;
  private TextVar wApplicationField;
  private TextVar wWorkflowField;
  private TextVar wTokenField;
  private TextVar wIdField;

  private TableView wFields;

  private boolean changed;

  public KsaarOutputDialog(
      Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname) {
    super(parent, variables, (BaseTransformMeta) in, pipelineMeta, sname);
    meta = (KsaarOutputMeta) in;
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
    shell.setText(BaseMessages.getString(PKG, "KsaarOutputDialog.Shell.Title"));

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

    // Spacer between entry info and content
    Label topSpacer = new Label(contentComposite, SWT.HORIZONTAL | SWT.SEPARATOR);
    FormData fdSpacer =
        new FormDataBuilder().fullWidth().top(wTransformNameField, MARGIN_SIZE).result();
    topSpacer.setLayoutData(fdSpacer);

    // --------------------- CONTENT --------------------- \\

    //    CTabFolder wTabDetail = new CTabFolder(shell, SWT.BORDER);
    //    props.setLook(wTabDetail, Props.WIDGET_STYLE_TAB);
    //
    //    CTabItem wDetailTab = new CTabItem(wTabDetail, SWT.NONE);
    //    wDetailTab.setText( BaseMessages.getString( PKG, "KsaarOutputDialog.DetailTab.TabTitle" )
    // );
    //
    //    Composite wDetailComp = new Composite(wTabDetail, SWT.NONE );
    //    props.setLook( wDetailComp );

    // Create a label for the "Token" field
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

    // Create a label for the "Application" field
    Label wApplicationLabel = new Label(contentComposite, SWT.RIGHT);
    wApplicationLabel.setText(
        BaseMessages.getString(PKG, "KsaarOutputDialog.ApplicationName.Label"));
    props.setLook(wApplicationLabel);
    FormData fdApplicationLabel =
        new FormDataBuilder()
            .left()
            .top(wTokenField, ELEMENT_SPACING)
            .right(middle, -ELEMENT_SPACING)
            .result();
    wApplicationLabel.setLayoutData(fdApplicationLabel);

    // Create the TextVar widget for "Workflow name"
    wApplicationField =
        new TextVar(variables, contentComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wApplicationField.setText(""); // Default text can be set here, or leave it blank
    props.setLook(wApplicationField);
    FormData fdApplication =
        new FormDataBuilder()
            .left(middle, 0)
            .top(wTokenField, ELEMENT_SPACING)
            .right(100, 0)
            .result();
    wApplicationField.setLayoutData(fdApplication);

    // ------------------------ \\

    // Create a label for the "Workflow" field
    Label wWorkflowLabel = new Label(contentComposite, SWT.RIGHT);
    wWorkflowLabel.setText(BaseMessages.getString(PKG, "KsaarOutputDialog.WorkflowName.Label"));
    props.setLook(wWorkflowLabel);
    FormData fdWorkflowLabel =
        new FormDataBuilder()
            .left()
            .top(wApplicationField, ELEMENT_SPACING)
            .right(middle, -ELEMENT_SPACING)
            .result();
    wWorkflowLabel.setLayoutData(fdWorkflowLabel);

    // Create the TextVar widget for "Workflow name"
    wWorkflowField = new TextVar(variables, contentComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wWorkflowField.setText(""); // Default text can be set here, or leave it blank
    props.setLook(wWorkflowField);
    FormData fdWorkflow =
        new FormDataBuilder()
            .left(middle, 0)
            .top(wApplicationField, ELEMENT_SPACING)
            .right(100, 0)
            .result();
    wWorkflowField.setLayoutData(fdWorkflow);

    // ------------------------ \\

    // Create a label for the "BulkType" field
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

    // Create the TextVar widget for "Workflow name"
    wBulkTypeField = new CCombo(contentComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wBulkTypeField.setText(""); // Default text can be set here, or leave it blank
    props.setLook(wBulkTypeField);
    FormData fdBulkType =
        new FormDataBuilder()
            .left(middle, 0)
            .top(wWorkflowField, ELEMENT_SPACING)
            .right(100, 0)
            .result();
    wBulkTypeField.setLayoutData(fdBulkType);

    // ------------------------ \\

    // Create a label for the "Email" field
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

    // Create the TextVar widget for "Workflow name"
    wEmailField = new TextVar(variables, contentComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wEmailField.setText(""); // Default text can be set here, or leave it blank
    props.setLook(wEmailField);
    FormData fdEmail =
        new FormDataBuilder()
            .left(middle, 0)
            .top(wBulkTypeField, ELEMENT_SPACING)
            .right(100, 0)
            .result();
    wEmailField.setLayoutData(fdEmail);

    // ------------------------ \\

    // Create a label for the "Email" field
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

    // Create the TextVar widget for "Id name"
    wIdField = new TextVar(variables, contentComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wIdField.setText(""); // Default text can be set here, or leave it blank
    props.setLook(wIdField);
    FormData fdId =
        new FormDataBuilder()
            .left(middle, 0)
            .top(wEmailField, ELEMENT_SPACING)
            .right(100, 0)
            .result();
    wIdField.setLayoutData(fdId);

    // ------------------------ \\

    //    CTabFolder wTabFields = new CTabFolder(shell, SWT.BORDER);
    //    props.setLook(wTabFields, Props.WIDGET_STYLE_TAB);
    //
    //    CTabItem wFieldsTab = new CTabItem(wTabFields, SWT.NONE);
    //    wFieldsTab.setText( BaseMessages.getString( PKG, "KsaarOutputDialog.FieldsTab.TabTitle" )
    // );
    //
    //    Composite wFieldsComp = new Composite(wTabFields, SWT.NONE );
    //    props.setLook( wFieldsComp );
    //
    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = Const.FORM_MARGIN;
    fieldsLayout.marginHeight = Const.FORM_MARGIN;

    int FieldsRows = 1;

    if (meta.getFields() != null) {
      FieldsRows = meta.getFields().size();
    }

    ColumnInfo[] colinf =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "KsaarOutputDialog.NameColumn.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {""},
              false),
        };

    wFields =
        new TableView(
            variables,
            contentComposite,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            colinf,
            FieldsRows,
            lsMod,
            props);
    FormData fdFields =
        new FormDataBuilder().left(middle, 0).top(wIdField, ELEMENT_SPACING).right(100, 0).result();
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

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void getData() {
    wBulkTypeField.add("CREATE");
    wBulkTypeField.add("UPDATE");
    wBulkTypeField.add("DELETE");

    String tokenField = meta.getTokenField();
    if (tokenField != null) wTokenField.setText(tokenField);

    String applicationField = meta.getApplicationField();
    if (applicationField != null) wApplicationField.setText(applicationField);

    String workflowField = meta.getWorkflowField();
    if (workflowField != null) wWorkflowField.setText(workflowField);

    String emailField = meta.getEmailField();
    if (emailField != null) wEmailField.setText(emailField);

    String idField = meta.getIdField();
    if (idField != null) wIdField.setText(idField);

    List<String> fields = meta.getFields();
    if (fields != null) {
      Table fieldsTable = wFields.table;

      for (int i = 0; i < fields.size(); i++) {
        TableItem item = fieldsTable.getItem(i);
        item.setText(1, fields.get(i));
      }
    }
  }

  private void setMeta(KsaarOutputMeta meta) {
    List<IStream> targetStreams = meta.getTransformIOMeta().getTargetStreams();

    meta.setBulkTypeField(wBulkTypeField.getText());
    meta.setTokenField(wTokenField.getText());
    meta.setApplicationField(wApplicationField.getText());
    meta.setWorkflowField(wWorkflowField.getText());
    meta.setEmailField(wEmailField.getText());
    meta.setIdField(wIdField.getText());

    Table fieldsTable = wFields.table;
    List<String> fields = new ArrayList<String>();

    for (int i = 0; i < fieldsTable.getItemCount(); i++) {
      TableItem item = fieldsTable.getItem(i);
      fields.add(item.getText(1));
    }

    meta.setFields(fields);
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

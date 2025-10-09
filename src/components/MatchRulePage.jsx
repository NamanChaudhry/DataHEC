import React, { useEffect, useState } from "react";
import {
  Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Paper,
  Button, TextField, Divider, IconButton, Box, Typography, Chip, Dialog,
  DialogTitle, DialogContent, DialogActions
} from "@mui/material";
import EditIcon from "@mui/icons-material/Edit";
import DeleteIcon from "@mui/icons-material/Delete";
import WorkingColumnMapping from "./WorkingColumnMapping";

const MatchRulePage = () => {
  const [rules, setRules] = useState([]);
  const [editingIndex, setEditingIndex] = useState(null);
  const [editValue, setEditValue] = useState({ rule: "", description: "" });
  const [addDialogOpen, setAddDialogOpen] = useState(false);
  const [newRule, setNewRule] = useState({ rule: "", description: "" });
  const [crossSystemEnabled, setCrossSystemEnabled] = useState(false);

  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [ruleToDelete, setRuleToDelete] = useState(null);

  // const staticColumns = [
  //   'Cust_Id', 'Source_System', 'First_Name', 'Last_Name', 'Company_Name',
  //   'Address', 'City', 'County', 'State', 'Zip', 'Phone1',
  //   'Phone2', 'Email', 'Web', 'Transaction_Date'
  // ];

  const staticColumns = [
    "Source System",
    "Customer Number",
    "Customer Name",
    "Customer Source Reference",
    "Taxpayer ID",
    "Taxpayer Registration Number",
    "Account Number",
    "Account Source Reference",
    "Account Type",
    "Account Description",
    "Account Established Date",
    "Customer Profile Class",
    "Capital IQ ID",
    "Transaction Activity Date",
    "Site Number",
    "Site Name",
    "Site Source Reference",
    "Account Address Set",
    "Location Source Reference",
    "Address Line 1",
    "Address Line 2",
    "Address Line 3",
    "Address Line 4",
    "City",
    "State",
    "Province",
    "Postal Code",
    "County",
    "Country",
    "Purpose",
    "Person Number",
    "Person Source Reference",
    "Salutary Introduction",
    "First Name",
    "Middle Name",
    "Last Name",
    "Job Title",
    "Responsibility Type",
    "Phone Number",
    "Phone Extension",
    "E-Mail Address",
    "Web URL"
  ];

  const [fuzzyColumns, setFuzzyColumns] = useState([]);
  const [exactColumns, setExactColumns] = useState([]);
  const [thresholds, setThresholds] = useState({});

  const handleMappingChange = (newFuzzy, newExact, newThresholds) => {
    setFuzzyColumns(newFuzzy);
    setExactColumns(newExact);
    setThresholds(newThresholds);
  };

  useEffect(() => {
    fetch("http://localhost:5001/api/match-rules")
      .then((res) => res.json())
      .then((data) => setRules(data))
      .catch((err) => console.error("Error loading match rules:", err));
  }, []);

  const handleEdit = (index) => {
    const selectedRule = rules[index];
    setEditingIndex(index);
    setEditValue({
      rule: selectedRule.rule,
      description: selectedRule.description,
    });
    setFuzzyColumns(selectedRule.fuzzy_columns || []);
    setExactColumns(selectedRule.exact_columns || []);
    setThresholds(selectedRule.thresholds || {});
  };

  const handleAddSave = async () => {
    try {
      const payload = {
        rule: newRule.rule,
        description: newRule.description,
        fuzzy_columns: fuzzyColumns,
        exact_columns: exactColumns,
        thresholds: thresholds,
      };
      const response = await fetch("http://localhost:5001/api/match-rules", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });

      const data = await response.json();

      if (response.ok) {
        setRules(data.rules);
        setAddDialogOpen(false);
        setNewRule({ rule: "", description: "" });
        setFuzzyColumns([]);
        setExactColumns([]);
        setThresholds({});
      } else {
        alert(data.error || "Failed to add rule");
      }
    } catch (err) {
      console.error("Error adding rule:", err);
    }
  };

  const handleSave = async () => {
    try {
      const oldRuleName = rules[editingIndex].rule;
      const response = await fetch("http://localhost:5001/api/match-rules", {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          oldRule: oldRuleName,
          rule: editValue.rule,
          description: editValue.description,
          fuzzy_columns: fuzzyColumns,
          exact_columns: exactColumns,
          thresholds: thresholds,
        }),
      });

      const data = await response.json();
      if (response.ok) {
        setRules(data.rules);
        setEditingIndex(null);
        setEditValue({ rule: "", description: "" });
        setFuzzyColumns([]);
        setExactColumns([]);
        setThresholds({});
      } else {
        alert(data.error || "Failed to update rule");
      }
    } catch (err) {
      console.error("Error updating rule:", err);
    }
  };

  // 🔹 Confirm delete (open dialog)
  const confirmDelete = (rule) => {
    setRuleToDelete(rule);
    setDeleteDialogOpen(true);
  };

  // 🔹 Perform delete
  const handleDelete = async () => {
    if (!ruleToDelete) return;
    try {
      const response = await fetch("http://localhost:5001/api/match-rules", {
        method: "DELETE",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ rule: ruleToDelete.rule }),
      });

      const data = await response.json();
      if (response.ok) {
        setRules(data.rules);
        setDeleteDialogOpen(false);
        setRuleToDelete(null);
      } else {
        alert(data.error || "Failed to delete rule");
      }
    } catch (err) {
      console.error("Error deleting rule:", err);
    }
  };

  return (
    <Box>
      <Box
        sx={{
          background: 'linear-gradient(135deg, #0d3965 0%, #205988 100%)',
          borderTopLeftRadius: 8,
          borderTopRightRadius: 8,
          p: 2,
        }}
      >
        <h1 style={{ fontSize: '1.5rem', fontFamily: 'Segoe UI', color: 'white', margin: 0 }}>
          Match Rules
        </h1>
      </Box>
      <Divider sx={{ backgroundColor: 'rgba(255,255,255,0.3)', mt: 1 }} />
      <Box mt={1.5} >
        <TableContainer
          component={Paper}
          sx={{
            mt: 1,
            mx: 0,
            boxShadow: 1,
            border: "0px solid #ddd",
            width: "100%",
            maxWidth: "100%",
            p: 1.2
          }}
        >
          <Table size="small">
            <TableHead>
              <TableRow>
                <TableCell sx={{ fontWeight: "bold", fontSize: '0.9rem', fontFamily: 'Arial' }}>Rule Name</TableCell>
                <TableCell sx={{ fontWeight: "bold", fontSize: '0.9rem', fontFamily: 'Arial' }}>Description</TableCell>
                <TableCell sx={{ fontWeight: "bold", fontSize: '0.9rem', fontFamily: 'Arial' }}>Fuzzy Columns</TableCell>
                <TableCell sx={{ fontWeight: "bold", fontSize: '0.9rem', fontFamily: 'Arial' }}>Exact Columns</TableCell>
                <TableCell sx={{ fontWeight: "bold", fontSize: '0.9rem', fontFamily: 'Arial' }}>Thresholds</TableCell>
                <TableCell align="right" sx={{ fontWeight: "bold", fontSize: '0.9rem', fontFamily: 'Arial' }}>Actions</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {rules.map((ruleItem, index) => (
                <TableRow key={index} sx={{ height: "1rem" }}>
                  <TableCell sx={{
                    py: 1.1,
                    maxWidth: 180,
                    whiteSpace: "normal",
                    wordBreak: "break-word",
                    fontFamily: 'sans-serif'
                  }}>{ruleItem.rule}</TableCell>
                  <TableCell
                    sx={{
                      py: 1.1,
                      maxWidth: 300,
                      whiteSpace: "normal",
                      wordBreak: "break-word",
                      fontFamily: 'sans-serif'
                    }}
                    title={ruleItem.description}
                  >
                    {ruleItem.description}
                  </TableCell>

                  <TableCell sx={{ py: 1.1, fontFamily: 'sans-serif' }}>
                    {ruleItem.fuzzy_columns?.join(", ") || "-"}
                  </TableCell>
                  <TableCell sx={{ py: 1.1, fontFamily: 'sans-serif' }}>
                    {ruleItem.exact_columns?.join(", ") || "-"}
                  </TableCell>
                  <TableCell sx={{ py: 1.1, fontFamily: 'sans-serif' }}>
                    {ruleItem.thresholds
                      ? Object.entries(ruleItem.thresholds)
                        .map(([key, value]) => `${key}: ${value}`)
                        .join(", ")
                      : "-"}
                  </TableCell>
                  <TableCell align="right" sx={{ py: 1 }}>
                    <Box sx={{ display: 'flex', justifyContent: 'flex-end', gap: 1 }}>
                      <IconButton size="small" onClick={() => confirmDelete(ruleItem)} style={{ color: 'red' }}>
                        <DeleteIcon fontSize="small" />
                      </IconButton>
                      <IconButton size="small" onClick={() => handleEdit(index)} style={{ color: 'GrayText' }}>
                        <EditIcon fontSize="small" />
                      </IconButton>
                    </Box>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>

        <Box display="flex" justifyContent="flex-end" mt={2}>
          <Button
            variant="white"
            sx={{ backgroundColor: "#0e304cff" }}
            onClick={() => setAddDialogOpen(true)}
            disabled={addDialogOpen}
          >
            Add New Rule
          </Button>
        </Box>
      </Box>

      {/* 🔹 Delete Confirmation Dialog */}
      <Dialog
        open={deleteDialogOpen}
        onClose={() => setDeleteDialogOpen(false)}
      >
        <DialogTitle>Confirm Delete</DialogTitle>
        <DialogContent>
          Are you sure you want to delete the rule <b>{ruleToDelete?.rule}</b>?
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setDeleteDialogOpen(false)}>Cancel</Button>
          <Button onClick={handleDelete} color="error" variant="contained">
            Delete
          </Button>
        </DialogActions>
      </Dialog>
      {/* Add Rule Dialog */}
      <Dialog
        open={addDialogOpen}
        onClose={() => setAddDialogOpen(false)}
        maxWidth="md"
        fullWidth
      >
        <DialogTitle style={{ fontSize: '1.5rem' }}>Select Rule Configuration</DialogTitle>
        <DialogContent dividers sx={{ pt: 3, px: 3, pb: 1 }}>
          <Box display="flex" justifyContent="space-between" alignItems="flex-start" flexWrap="wrap" gap={2}>
            {/* Rule Name & Description Fields */}
            <Box flexGrow={1} display="flex" gap={2} flexWrap="wrap">
              <TextField
                label="Rule Name"
                variant="outlined"
                value={newRule.rule}
                onChange={(e) => setNewRule({ ...newRule, rule: e.target.value })}
                placeholder="Enter rule name"
                sx={{ minWidth: 250, flex: 1 }}
              />
              <TextField
                label="Description"
                variant="outlined"
                value={newRule.description}
                onChange={(e) => setNewRule({ ...newRule, description: e.target.value })}
                multiline
                rows={1}
                placeholder="Enter rule description"
                sx={{ minWidth: 250, flex: 2 }}
              />
            </Box>
          </Box>

          {/* File Info Chips */}
          <Box mt={3}>
            {/* <Typography variant="h6">Source System / File Name</Typography> */}
            <Box sx={{ display: "flex", gap: 1, mt: 1 }}>
              {/* <Chip size="small" label="filetype" color="default" /> */}
              <Chip size="small" label={`Fuzzy: ${fuzzyColumns.length}`} variant="outlined" />
              <Chip size="small" label={`Exact: ${exactColumns.length}`} variant="outlined" />
            </Box>
          </Box>

          {/* Column Mapping Component */}
          {!crossSystemEnabled && (
            <Box mt={3}>
              <WorkingColumnMapping
                editable={true}
                columns={staticColumns}
                fuzzyColumns={fuzzyColumns}
                exactColumns={exactColumns}
                thresholds={thresholds}
                onMappingChange={handleMappingChange}
              />
            </Box>
          )}

          {/* Info Note */}
          <Box
            sx={{
              p: 2,
              bgcolor: "#f0f7ff",
              borderRadius: 1,
              mt: 2,
              display: "flex",
              flexDirection: "column",
              alignItems: "center",
              justifyContent: "center",
              textAlign: "center",
            }}
          >
            <Typography variant="body2" color="text.secondary">
              📋 Available columns:
            </Typography>
            <Typography variant="caption" color="primary">
              File type can be changed above. Column configuration will be set globally for all files.
            </Typography>
          </Box>

        </DialogContent>
        <DialogActions>
          <Button onClick={() => setAddDialogOpen(false)} variant="" color="secondary">
            Cancel
          </Button>
          <Button onClick={handleAddSave} variant="contained" color="primary">
            Save Rule
          </Button>
        </DialogActions>
      </Dialog>

      {/* Edit Rule Dialog */}
      <Dialog
        open={editingIndex !== null}
        onClose={() => setEditingIndex(null)}
        maxWidth="md"
        fullWidth
      >
        <DialogTitle>Edit Rule Configuration</DialogTitle>
        <DialogContent dividers sx={{ pt: 1, px: 3, pb: 1 }}>
          <Box display="flex" justifyContent="space-between" alignItems="flex-start" flexWrap="wrap" gap={2}>
            {/* Rule Name & Description Fields */}
            <Box flexGrow={1} display="flex" gap={1} flexWrap="wrap">
              <TextField
                fullWidth
                label="Rule"
                value={editValue.rule}
                onChange={(e) => setEditValue({ ...editValue, rule: e.target.value })}
                variant="outlined"
                margin="dense"
              />
              <TextField
                fullWidth
                label="Description"
                value={editValue.description}
                onChange={(e) => setEditValue({ ...editValue, description: e.target.value })}
                variant="outlined"
                margin="dense"
                multiline
                rows={3}
              />
            </Box>
          </Box>
          <Box mt={3}>
            {/* <Typography variant="h6">Source System / File Name</Typography> */}
            <Box sx={{ display: "flex", gap: 1, mt: 1 }}>
              {/* <Chip size="small" label="filetype" color="default" /> */}
              <Chip size="small" label={`Fuzzy: ${fuzzyColumns.length}`} variant="outlined" />
              <Chip size="small" label={`Exact: ${exactColumns.length}`} variant="outlined" />
            </Box>
          </Box>

          {/* Column Mapping Component */}
          {!crossSystemEnabled && (
            <Box mt={3}>
              <WorkingColumnMapping
                editable={true}
                columns={staticColumns}
                fuzzyColumns={fuzzyColumns}
                exactColumns={exactColumns}
                thresholds={thresholds}
                onMappingChange={handleMappingChange}
              />
            </Box>
          )}

          {/* Info Note */}
          <Box
            sx={{
              p: 2,
              bgcolor: "#f0f7ff",
              borderRadius: 1,
              mt: 2,
              display: "flex",
              flexDirection: "column",
              alignItems: "center",
              justifyContent: "center",
              textAlign: "center",
            }}
          >
            <Typography variant="body2" color="text.secondary">
              📋 Available columns:
            </Typography>
            <Typography variant="caption" color="primary">
              File type can be changed above. Column configuration will be set globally for all files.
            </Typography>
          </Box>

        </DialogContent>
        <DialogActions style={{ margin: '0.5rem' }}>
          <Button onClick={() => setEditingIndex(null)}>Cancel</Button>
          <Button onClick={handleSave} variant="contained" color="primary">
            Save
          </Button>
        </DialogActions>
      </Dialog>
    </Box >
  );
};

export default MatchRulePage;

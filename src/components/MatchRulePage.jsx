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

  const staticColumns = [
    'Cust_Id', 'Source_System', 'First_Name', 'Last_Name', 'Company_Name',
    'Address', 'City', 'County', 'State', 'Zip', 'Phone1',
    'Phone2', 'Email', 'Web', 'Transaction_Date'
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
    setEditingIndex(index);
    setEditValue(rules[index]);
  };

  const handleAddSave = async () => {
    try {
      const response = await fetch("http://localhost:5001/api/match-rules", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(newRule),
      });

      const data = await response.json();

      if (response.ok) {
        setRules(data.rules);
        setAddDialogOpen(false);
        setNewRule({ rule: "", description: "" });
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
        }),
      });

      const data = await response.json();
      if (response.ok) {
        setRules(data.rules);
        setEditingIndex(null);
        setEditValue({ rule: "", description: "" });
      } else {
        alert(data.error || "Failed to update rule");
      }
    } catch (err) {
      console.error("Error updating rule:", err);
    }
  };

  return (
    <Box sx={{ mx: "auto" }}>
      <h1 style={{ fontSize: '1.5rem' }}>Match Rules</h1>
      <Divider />

      <Box mt={1.5}>
        <TableContainer
          component={Paper}
          sx={{
            mt: 1,
            mx: 0,
            boxShadow: 1,
            border: "1px solid #ddd",
            width: "100%",
            maxWidth: "100%",
          }}
        >
          <Table size="small">
            <TableHead>
              <TableRow>
                <TableCell sx={{ fontWeight: "bold" }}>Rule Name</TableCell>
                <TableCell sx={{ fontWeight: "bold" }}>Description</TableCell>
                <TableCell align="right" sx={{ fontWeight: "bold" }}>Action</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {rules.map((ruleItem, index) => (
                <TableRow key={index} sx={{ height: "2rem" }}>
                  <TableCell sx={{ py: 2 }}>{ruleItem.rule}</TableCell>
                  <TableCell sx={{ py: 2 }}>{ruleItem.description}</TableCell>
                  <TableCell align="right" sx={{ py: 1 }}>
                    <IconButton size="small" onClick={() => handleEdit(index)}>
                      <EditIcon fontSize="small" />
                    </IconButton>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>

        <Box display="flex" justifyContent="flex-end" mt={2}>
          <Button
            variant="white"
            sx={{ backgroundColor: "#175c93ff" }}
            onClick={() => setAddDialogOpen(true)}
            disabled={addDialogOpen}
          >
            Add New Rule
          </Button>
        </Box>
      </Box>

      {/* Add Rule Dialog */}
      <Dialog
        open={addDialogOpen}
        onClose={() => setAddDialogOpen(false)}
        maxWidth="lg"
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
              <Chip size="small" label="filetype" color="default" />
              <Chip size="small" label={`Fuzzy: ${fuzzyColumns.length}`} variant="outlined" />
              <Chip size="small" label={`Exact: ${exactColumns.length}`} variant="outlined" />
            </Box>
          </Box>

          {/* Column Mapping Component */}
          {!crossSystemEnabled && (
            <Box mt={3}>
              <WorkingColumnMapping
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
        maxWidth="sm"
        fullWidth
      >
        <DialogTitle>Edit Match Rule</DialogTitle>
        <DialogContent>
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
        </DialogContent>
        <DialogActions style={{margin:'0.5rem'}}>
          <Button onClick={() => setEditingIndex(null)}>Cancel</Button>
          <Button onClick={handleSave} variant="contained" color="primary">
            Save
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default MatchRulePage;

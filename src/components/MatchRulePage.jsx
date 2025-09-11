import React, { useEffect, useState } from "react";
import {
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  Button,
  TextField,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Divider,
  IconButton,
} from "@mui/material";
import EditIcon from "@mui/icons-material/Edit";

const MatchRulePage = () => {
  const [rules, setRules] = useState([]);
  const [editingIndex, setEditingIndex] = useState(null);
  const [editValue, setEditValue] = useState({ rule: "", description: "" });

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
    <div>
      <h1 style={{ fontSize: "1.8rem" }}>Match Rules</h1>
      <Divider style={{ marginBottom: "1rem" }} />

      <TableContainer component={Paper}>
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell style={{ fontWeight: "bold" }}>Rule Name</TableCell>
              <TableCell style={{ fontWeight: "bold" }}>Description</TableCell>
              <TableCell align="right" style={{ fontWeight: "bold" }}>
                Action
              </TableCell>
            </TableRow>
          </TableHead>

          <TableBody>
            {rules.map((ruleItem, index) => (
              <TableRow
                key={index}
                sx={{ height: "2rem" }}
              >
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


      {/* Edit Dialog */}
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
            onChange={(e) =>
              setEditValue({ ...editValue, rule: e.target.value })
            }
            variant="outlined"
            margin="dense"
          />
          <TextField
            fullWidth
            label="Description"
            value={editValue.description}
            onChange={(e) =>
              setEditValue({ ...editValue, description: e.target.value })
            }
            variant="outlined"
            margin="dense"
            multiline
            rows={3}
          />
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setEditingIndex(null)}>Cancel</Button>
          <Button onClick={handleSave} variant="contained" color="primary">
            Save
          </Button>
        </DialogActions>
      </Dialog>
    </div>
  );
};

export default MatchRulePage;

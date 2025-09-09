// src/components/FileSystemMappingItem.jsx
import React, { useEffect, useState } from 'react';
import {
  Box,
  Stack,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Button,
  Typography,
  Divider,
  Paper,
  CircularProgress
} from '@mui/material';
import AddIcon from '@mui/icons-material/Add';
import UploadFileIcon from '@mui/icons-material/UploadFile';
import MergeRuleSelector from './Mergeruleselector';
import Checkbox from '@mui/material/Checkbox';
import ListItemText from '@mui/material/ListItemText';
import Dialog from "@mui/material/Dialog";
import DialogTitle from "@mui/material/DialogTitle";
import DialogContent from "@mui/material/DialogContent";
import DialogActions from "@mui/material/DialogActions";
import TextField from "@mui/material/TextField";
import AutorenewIcon from '@mui/icons-material/Autorenew';
import { ActivityIcon, Edit, Edit2Icon, Edit3Icon, EditIcon } from 'lucide-react';


const FileSystemMappingItem = ({
  entity,
  sourceSystems,
  selectedSourceSystem,
  setSelectedSourceSystem,
  files,
  selectedFile,
  setSelectedFile,
  availableFiles,
  setAvailableFiles,
  onAddFile
}) => {
  console.log('FileSystemMappingItem rendered with files:', files);

  const [rules, setRules] = useState([]);
  const [selectedRule, setSelectedRule] = useState([]);
  const [mergeRule, setMergeRule] = useState([]);
  const [resetDropdowns, setResetDropdowns] = useState(false);
  const [crossSystemEnabled, setCrossSystemEnabled] = useState(false);
  const [openDialog, setOpenDialog] = useState(false);
  const [newRule, setNewRule] = useState("");

  const [openEditDialog, setOpenEditDialog] = useState(false);
  const [editingIndex, setEditingIndex] = useState(null);
  const [editValue, setEditValue] = useState("");

  useEffect(() => {
    fetch("http://localhost:5001/api/match-rules")
      .then((res) => res.json())
      .then((data) => setRules(data))
      .catch((err) => console.error("Error loading match rules:", err));
  }, []);

  const handleAddRule = async () => {
    const newRule = prompt("Enter new match rule:");
    if (!newRule) return;

    try {
      const response = await fetch("http://localhost:5001/api/match-rules", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ rule: newRule }),
      });

      const data = await response.json();
      if (response.ok) {
        setRules(data.rules); // update rules state
      } else {
        alert(data.error || "Failed to add rule");
      }
    } catch (err) {
      console.error("Error adding rule:", err);
    }
  };


  // useEffect(() => {
  //   fetch('/utils/matchrules.json')
  //     .then((res) => res.json())
  //     .then((data) => setRules(data))
  //     .catch((err) => console.error('Error loading match rules:', err));
  // }, []);

  useEffect(() => {
    if (resetDropdowns) {
      setSelectedRule([]);
      setMergeRule([]);
      setResetDropdowns(false);
    }
  }, [resetDropdowns]);

  return (
    <Box sx={{ width: '100%', overflow: 'visible' }}>
      <Stack spacing={2}>
        <Divider />
        {/* Source System Row */}
        <Box>
          <Typography variant="subtitle1" sx={{ fontWeight: 600, color: '#37474f', mb: 1 }}>
            🗂️ Source System
          </Typography>
          <FormControl fullWidth variant="outlined"
            sx={{
              '& .MuiOutlinedInput-root': {
                borderRadius: '12px',
                backgroundColor: '#e3f2fd',
                boxShadow: '0 2px 6px rgba(0,0,0,0.1)',
                '& fieldset': { borderColor: '#90caf9' },
                '&:hover fieldset': { borderColor: '#42a5f5' },
                '&.Mui-focused fieldset': { borderColor: '#1976d2', borderWidth: '2px' },
              },
              '& .MuiSelect-select': {
                padding: '14px',
                fontSize: '1rem',
                fontWeight: 500,
                color: '#0d47a1',
              },
            }}
          >
            <InputLabel sx={{ fontWeight: 500, fontSize: 12, color: '#1976d2' }}>
              Select source system
            </InputLabel>
            <Select
              value={selectedSourceSystem}
              onChange={(e) => setSelectedSourceSystem(e.target.value)}
              disabled={!entity}
              label="Select source system..."
              MenuProps={{
                PaperProps: {
                  sx: {
                    bgcolor: '#ffffff', // white background for dropdown
                    borderRadius: 2,
                    boxShadow: '0 4px 12px rgba(0,0,0,0.15)',
                    '& .MuiMenuItem-root': {
                      fontWeight: 500,
                      color: '#000000', // black textF
                      bgcolor: '#ffffff', // white background
                      '&:hover': {
                        bgcolor: '#e3f2fd', // light blue on hover
                        color: '#1976d2',
                      },
                      '&.Mui-selected': {
                        bgcolor: '#1976d2', // blue background when selected
                        color: '#ffffff', // white text when selected
                        '&:hover': {
                          bgcolor: '#1565c0', // darker blue on hover
                        },
                      },
                    },
                  },
                },
              }}
              sx={{
                '& .MuiSelect-select': {
                  padding: '14px',
                  fontSize: '1rem',
                  fontWeight: 500,
                  color: '#0d47a1',
                  backgroundColor: '#e3f2fd',
                  borderRadius: '12px',
                },
              }}
            >
              <MenuItem value="">
                <em>Select source system...</em>
              </MenuItem>
              {sourceSystems.map((system) => (
                <MenuItem key={system} value={system}>
                  {system}
                </MenuItem>
              ))}
            </Select>
          </FormControl>
        </Box>

        {/* File Row - Enhanced for better visibility */}
        <Box>
          <Typography variant="subtitle1" sx={{ fontWeight: 600, color: '#37474f', mb: 1 }}>
            📤 Upload File
          </Typography>
          <FormControl fullWidth>
            <Button
              variant="outlined"
              component="label"
              startIcon={<UploadFileIcon />}
              disabled={!selectedSourceSystem}
              sx={{
                padding: '12px 18px',
                border: '2px dashed #90caf9',
                backgroundColor: '#f1f8e9',
                color: '#558b2f',
                '&:hover': {
                  backgroundColor: '#dcedc8',
                  borderColor: '#7cb342',
                }
              }}
            >
              {selectedSourceSystem ? `Upload File for ${selectedSourceSystem}` : 'Upload File'}
              <input
                type="file"
                hidden
                onChange={(e) => {
                  const file = e.target.files[0];
                  if (file) {
                    setSelectedFile(file.name);
                    console.log('Selected file:', file.name);

                    setAvailableFiles(prev => [
                      ...prev,
                      {
                        name: file.name,
                        type: file.name.includes('_Output') ? 'output' : 'source',
                        displayName: file.name
                      }
                    ]);
                  }
                }}
                accept=".csv,.xlsx,.json"
              />
            </Button>
          </FormControl>

          {selectedFile && (
            <Box sx={{
              mt: 2,
              p: 2,
              bgcolor: '#e8f5e9',
              borderRadius: 2,
              border: '2px solid #4caf50'
            }}>
              <Typography variant="caption" color="text.secondary">
                ✅ Selected File:
              </Typography>
              <Typography
                variant="body2"
                sx={{
                  fontWeight: 500,
                  wordBreak: 'break-word',
                  mt: 0.5,
                  color: '#2e7d32'
                }}
              >
                {selectedFile}
              </Typography>
            </Box>
          )}
        </Box>

        <Box>
          <Typography variant="subtitle1" sx={{ fontWeight: 600, color: '#37474f', mb: 1 }}>
            🎯 Match Rule
          </Typography>

          {/* Dropdown */}
          <FormControl fullWidth variant="outlined"
            sx={{
              '& .MuiOutlinedInput-root': {
                borderRadius: '12px',
                backgroundColor: '#e3f2fd',
                boxShadow: '0 2px 6px rgba(0,0,0,0.1)',
                '& fieldset': { borderColor: '#90caf9' },
                '&:hover fieldset': { borderColor: '#42a5f5' },
                '&.Mui-focused fieldset': { borderColor: '#1976d2', borderWidth: '2px' },
              },
            }}
          >
            <InputLabel id="match-rule-label" sx={{ fontWeight: 500, fontSize: 12, color: '#1976d2' }}>
              Select match rule
            </InputLabel>

            <Select
              labelId="match-rule-label"
              multiple
              value={selectedRule}
              onChange={(e) =>
                setSelectedRule(
                  typeof e.target.value === "string"
                    ? e.target.value.split(",")
                    : e.target.value
                )
              }
              label="Match Rule"
              renderValue={(selected) => selected.join(", ")}
            >
              {rules.length === 0 ? (
                <MenuItem disabled>
                  <CircularProgress size={20} sx={{ mr: 1 }} /> Loading rules...
                </MenuItem>
              ) : (
                rules.map((rule, index) => (
                  <MenuItem key={index} value={rule}>
                    <Checkbox checked={selectedRule.indexOf(rule) > -1} />
                    <ListItemText primary={rule} />
                  </MenuItem>
                ))
              )}
            </Select>
          </FormControl>

          {/* Button aligned to right, below dropdown */}
          {/* Button row aligned to right, below dropdown */}
          <Box sx={{ display: "flex", justifyContent: "flex-end", mt: 1, gap: 1 }}>
            {/* Edit Rule */}
            <Button
              variant="outlined"
              size="small"
              onClick={() => setOpenEditDialog(true)}
              sx={{
                borderRadius: "8px",
                textTransform: "none",
                borderColor: "#ff9800",
                color: "#ef6c00",
                "&:hover": { borderColor: "#ef6c00", backgroundColor: "#fff3e0" },
              }}
              startIcon={<Edit />}
            >
              Edit Rules
            </Button>
            {/* New Rule */}
            <Button
              variant="outlined"
              size="small"
              onClick={() => {
                setNewRule(""); // empty for new
                setOpenDialog(true);
              }}
              sx={{
                borderRadius: "8px",
                textTransform: "none",
                borderColor: "#42a5f5",
                color: "#1976d2",
                "&:hover": {
                  borderColor: "#1e88e5",
                  backgroundColor: "#e3f2fd",
                },
              }}
              startIcon={<AddIcon fontSize="small" />}
            >
              Add Rule
            </Button>

          </Box>

          {/* <Box sx={{ display: "flex", justifyContent: "flex-end", mt: 1 }}>
            <Button
              variant="outlined"
              color="primary"
              size="small"
              onClick={() => setOpenDialog(true)}
              sx={{
                borderRadius: "8px",
                textTransform: "none",
                borderColor: "#42a5f5",
                color: "#1976d2",
                "&:hover": {
                  borderColor: "#1e88e5",
                  backgroundColor: "#e3f2fd",
                },
              }}
              startIcon={<AddIcon fontSize="small" />}
            >
              New Rule
            </Button>
          </Box> */}
        </Box>

        {/* <Button
          variant="outlined"
          color="secondary"
          onClick={() => setOpenDialog(true)}
          sx={{ ml: 2 }}
        >
          + New Rule
        </Button> */}

        <MergeRuleSelector
          isCrossSystem={false}
          mergeRule={mergeRule}
          onChange={setMergeRule}
        />

        {/* Button Row */}
        <Box sx={{ pt: 1 }}>
          <Button
            variant="contained"
            color="primary"
            startIcon={<AutorenewIcon />}
            onClick={() => {
              onAddFile?.();
              setResetDropdowns(true);
            }}
            disabled={!selectedSourceSystem || !selectedFile || selectedRule.length === 0 || mergeRule.length === 0}
            size="large"
            sx={{
              width: 'fit-content',
              minWidth: '130px',
              padding: '3px 8px',
              fontSize: '0.9rem',
              borderRadius: '10px',
              background: 'linear-gradient(45deg, #42a5f5 30%, #1e88e5 90%)',
              color: '#fff',
              boxShadow: '0 3px 5px 2px rgba(33, 150, 243, .3)',
              '&:hover': {
                background: 'linear-gradient(45deg, #1e88e5 30%, #1565c0 90%)',
              }
            }}
          >
            Process
          </Button>
        </Box>
        <Dialog
          open={openDialog}
          onClose={() => setOpenDialog(false)}
          PaperProps={{
            sx: {
              borderRadius: "16px",
              p: 2,
              boxShadow: "0 8px 24px rgba(0,0,0,0.2)",
            },
          }}
        >
          <DialogTitle
            sx={{
              fontWeight: 600,
              fontSize: "1.2rem",
              textAlign: "center",
              color: "#1976d2",
              pb: 2,
            }}
          >
            Add New Match Rule
          </DialogTitle>

          <DialogContent>
            <TextField
              autoFocus
              margin="dense"
              label="Rule Name"
              fullWidth
              variant="outlined"
              value={newRule}
              onChange={(e) => setNewRule(e.target.value)}
              InputLabelProps={{
                shrink: true,
              }}
              sx={{
                "& .MuiOutlinedInput-root": {
                  borderRadius: "12px",
                  backgroundColor: "#f5f9ff",
                  "& fieldset": { borderColor: "#90caf9" },
                  "&:hover fieldset": { borderColor: "#42a5f5" },
                  "&.Mui-focused fieldset": {
                    borderColor: "#1976d2",
                    borderWidth: 2,
                  },
                },
                "& .MuiInputLabel-root": {
                  fontSize: "0.9rem",
                  fontWeight: 500,
                  color: "#1976d2",
                  transform: "translate(14px, -6px) scale(0.85)",
                },
              }}
            />

          </DialogContent>

          <DialogActions sx={{ px: 3, pb: 2, justifyContent: "space-between" }}>
            <Button
              onClick={() => setOpenDialog(false)}
              variant="outlined"
              sx={{
                borderRadius: "10px",
                textTransform: "none",
                color: "#1976d2",
                borderColor: "#42a5f5",
                "&:hover": { borderColor: "#1e88e5", background: "#e3f2fd" },
              }}
            >
              Cancel
            </Button>
            <Button
              onClick={async () => {
                if (!newRule.trim()) return;
                try {
                  const response = await fetch("http://localhost:5001/api/match-rules", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ rule: newRule }),
                  });
                  const data = await response.json();
                  if (response.ok) {
                    setRules(data.rules);
                    setNewRule("");
                    setOpenDialog(false);
                  } else {
                    alert(data.error || "Failed to add rule");
                  }
                } catch (err) {
                  console.error("Error adding rule:", err);
                }
              }}
              variant="contained"
              sx={{
                borderRadius: "10px",
                textTransform: "none",
                background: "linear-gradient(45deg, #42a5f5 30%, #1e88e5 90%)",
                boxShadow: "0px 4px 12px rgba(33, 150, 243, 0.4)",
                "&:hover": {
                  background: "linear-gradient(45deg, #1e88e5 30%, #1565c0 90%)",
                },
              }}
            >
              Add
            </Button>
          </DialogActions>
        </Dialog>

        <Dialog
          open={openEditDialog}
          onClose={() => {
            setOpenEditDialog(false);
            setEditingIndex(null);
          }}
          PaperProps={{
            sx: { borderRadius: "16px", p: 2, minWidth: "550px" },
          }}
        >
          <DialogTitle sx={{ fontWeight: 600, color: "#1976d2" }}>
            Edit Match Rules
          </DialogTitle>
          <DialogContent dividers>
            {rules.map((rule, index) => (
              <Box
                key={index}
                sx={{
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "space-between",
                  mb: 1,
                  p: 1,
                  borderRadius: "8px",
                  bgcolor: "#f9f9f9",
                }}
              >
                {editingIndex === index ? (
                  <TextField
                    value={editValue}
                    onChange={(e) => setEditValue(e.target.value)}
                    size="small"
                    fullWidth
                    sx={{ mr: 1 }}
                  />
                ) : (
                  <Typography>{rule}</Typography>
                )}

                {editingIndex === index ? (
                  <Button
                    size="small"
                    variant="contained"
                    onClick={async () => {
                      if (!editValue.trim()) return;
                      try {
                        const response = await fetch("http://localhost:5001/api/match-rules", {
                          method: "PUT",
                          headers: { "Content-Type": "application/json" },
                          body: JSON.stringify({ oldRule: rule, rule: editValue }),
                        });
                        const data = await response.json();
                        if (response.ok) {
                          setRules(data.rules);
                          setEditingIndex(null);
                          setEditValue("");
                        } else {
                          alert(data.error || "Update failed");
                        }
                      } catch (err) {
                        console.error("Error updating rule:", err);
                      }
                    }}
                    sx={{ borderRadius: "8px" }}
                  >
                    Save
                  </Button>
                ) : (
                  <Button
                    size="small"
                    variant="outlined"
                    onClick={() => {
                      setEditingIndex(index);
                      setEditValue(rule);
                    }}
                    sx={{ borderRadius: "8px" }}
                  >
                    Edit
                  </Button>
                )}
              </Box>
            ))}
          </DialogContent>
          <DialogActions>
            <Button onClick={() => setOpenEditDialog(false)}>Close</Button>
          </DialogActions>
        </Dialog>

      </Stack>
    </Box>
  );
};

export default FileSystemMappingItem;

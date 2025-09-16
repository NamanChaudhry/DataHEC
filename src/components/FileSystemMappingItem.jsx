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
  CircularProgress
} from '@mui/material';
import UploadFileIcon from '@mui/icons-material/UploadFile';
import MergeRuleSelector from './Mergeruleselector';
import Checkbox from '@mui/material/Checkbox';
import ListItemText from '@mui/material/ListItemText';
import AutorenewIcon from '@mui/icons-material/Autorenew';
import WorkingColumnMapping from './WorkingColumnMapping';
import SelectedColumnsDisplay from './SelectedColumnsDisplay';


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
  const [newRule, setNewRule] = useState({ rule: "", description: "" });

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

  const handleProcess = () => {
    // Find the selected rule object (assuming single selection)
    const ruleObj = rules.find(r => r.rule === selectedRule[0]);
    if (!ruleObj) {
      alert("Selected rule not found!");
      return;
    }
    // Pass the rule config to parent
    onAddFile?.({
      fuzzyColumns: ruleObj.fuzzy_columns || [],
      exactColumns: ruleObj.exact_columns || [],
      thresholds: ruleObj.thresholds || {},
      selectedRule: selectedRule[0], // for reference
      mergeRule,
    });
    setResetDropdowns(true);
  };

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
              renderValue={(selected) => (
                <Box
                  sx={{
                    display: 'flex',
                    justifyContent: 'center',
                    alignItems: 'center',
                    height: '100%',
                    width: '100%',
                    textAlign: 'center',
                    flexWrap: 'wrap',
                  }}
                >
                  <Typography sx={{ fontWeight: 600, color: '#0d47a1'}}>
                    {selected.join(', ')}
                  </Typography>
                </Box>
              )}

            >
              {rules.length === 0 ? (
                <MenuItem disabled>
                  <CircularProgress size={20} sx={{ mr: 1 }} /> Loading rules...
                </MenuItem>
              ) : (
                rules.map((ruleObj, index) => (
                  <MenuItem key={index} value={ruleObj.rule}>
                    <Checkbox checked={selectedRule.includes(ruleObj.rule)} />
                    <ListItemText
                      primary={ruleObj.rule}
                      primaryTypographyProps={{ fontWeight: 500 }}
                      secondaryTypographyProps={{ fontSize: 12, color: 'text.secondary' }}
                    />
                  </MenuItem>
                ))
              )}
            </Select>
          </FormControl>
          <br></br>

          {/* Show selected fuzzy/exact columns below dropdown */}
          {selectedRule.length > 0 && (() => {
            // Aggregate fuzzy/exact columns and thresholds from all selected rules
            let fuzzyColumns = [];
            let exactColumns = [];
            let thresholds = {};

            selectedRule.forEach(ruleName => {
              const ruleObj = rules.find(r => r.rule === ruleName);
              if (ruleObj) {
                fuzzyColumns = [...fuzzyColumns, ...(ruleObj.fuzzy_columns || [])];
                exactColumns = [...exactColumns, ...(ruleObj.exact_columns || [])];
                thresholds = { ...thresholds, ...(ruleObj.thresholds || {}) };
              }
            });

            // Remove duplicates
            fuzzyColumns = [...new Set(fuzzyColumns)];
            exactColumns = [...new Set(exactColumns)];

            return (
              <SelectedColumnsDisplay
                fuzzyColumns={fuzzyColumns}
                exactColumns={exactColumns}
                thresholds={thresholds}
              />
            );
          })()}
        </Box>

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
            onClick={handleProcess}
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
      </Stack>
    </Box>
  );
};

export default FileSystemMappingItem;

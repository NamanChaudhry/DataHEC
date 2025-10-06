
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
  CircularProgress,
  IconButton,
  Paper
} from '@mui/material';
import UploadFileIcon from '@mui/icons-material/UploadFile';
import MergeRuleSelector from './Mergeruleselector';
import Checkbox from '@mui/material/Checkbox';
import ListItemText from '@mui/material/ListItemText';
import AutorenewIcon from '@mui/icons-material/Autorenew';
import SelectedColumnsDisplay from './SelectedColumnsDisplay';
import InsertDriveFileIcon from '@mui/icons-material/InsertDriveFile';
import CloseIcon from '@mui/icons-material/Close';
import ArrowDropDownIcon from '@mui/icons-material/ArrowDropDown';
import PlaylistAddCheckIcon from '@mui/icons-material/PlaylistAddCheck';

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
  onAddFile,
}) => {
  const [rules, setRules] = useState([]);
  const [selectedRule, setSelectedRule] = useState([]);
  const [mergeRule, setMergeRule] = useState([]);
  const [resetDropdowns, setResetDropdowns] = useState(false);
  const [selectedCategory, setSelectedCategory] = useState('');


  useEffect(() => {
    fetch('http://localhost:5001/api/match-rules')
      .then((res) => res.json())
      .then((data) => setRules(data))
      .catch((err) => console.error('Error loading match rules:', err));
  }, []);

  useEffect(() => {
    if (resetDropdowns) {
      setSelectedRule([]);
      setMergeRule([]);
      setSelectedCategory('');
      setResetDropdowns(false);
    }
  }, [resetDropdowns]);

  // const handleProcess = () => {
  //   const ruleObj = rules.find((r) => r.rule === selectedRule[0]);
  //   if (!ruleObj) {
  //     alert('Selected rule not found!');
  //     return;
  //   }
  //   onAddFile?.({
  //     fuzzyColumns: ruleObj.fuzzy_columns || [],
  //     exactColumns: ruleObj.exact_columns || [],
  //     thresholds: ruleObj.thresholds || {},
  //     selectedRule: selectedRule[0],
  //     mergeRule,
  //   });
  //   setResetDropdowns(true);
  // };

  const handleProcess = () => {
  if (!selectedSourceSystem) {
    alert("Please select a source system first");
    return;
  }
  if (!selectedCategory) {
    alert("Please select a category first");
    return;
  }
  if (selectedRule.length === 0) {
    alert("Please select at least one match rule");
    return;
  }
  if (mergeRule.length === 0) {
    alert("Please select at least one merge rule");
    return;
  }

  const ruleObj = rules.find((r) => r.rule === selectedRule[0]);
  onAddFile?.({
    fuzzyColumns: ruleObj.fuzzy_columns || [],
    exactColumns: ruleObj.exact_columns || [],
    thresholds: ruleObj.thresholds || {},
    selectedRule: selectedRule[0],
    mergeRule,
  });
  setResetDropdowns(true);
};


  return (
    <Box sx={{ width: '100%', overflow: 'visible' }}>
      <Stack spacing={3}>
        <Divider />

        {/* Row: Source System + Upload File */}
        <Stack direction="row" spacing={3} alignItems="flex-start">
          {/* Source System */}
          <Box flex={1}>
            <Typography variant="subtitle1" sx={{ fontWeight: 600, color: '#1976d2', mb: 1, }}>
              Source System
            </Typography>
            <FormControl
              fullWidth
              variant="outlined"
              sx={{
                '& .MuiOutlinedInput-root': {
                  borderRadius: '12px',
                  backgroundColor: '#2d284a',
                  boxShadow: '0 2px 6px rgba(0,0,0,0.1)',
                  '& fieldset': { borderColor: '#90caf9' },
                  '&:hover fieldset': { borderColor: '#42a5f5' },
                  '&.Mui-focused fieldset': { borderColor: '#1976d2', borderWidth: '2px' },
                },
                '& .MuiSelect-select': {
                  padding: '14px',
                  fontSize: '1rem',
                  fontWeight: 500,
                  color: '#a9a7a7ff',
                },
              }}
            >
              <InputLabel sx={{ fontWeight: 500, fontSize: 12, color: '#acc1d7ff' }}>
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
                      // bgcolor: '#2d284a',
                      borderRadius: 2,
                      boxShadow: '0 4px 12px rgba(0,0,0,0.15)',
                      '& .MuiMenuItem-root': {
                        fontWeight: 500,
                        color: '#000000',
                        bgcolor: '#ffffff',
                        '&:hover': {
                          bgcolor: '#e3f2fd',
                          color: '#1976d2',
                        },
                        '&.Mui-selected': {
                          bgcolor: '#1976d2',
                          color: '#ffffff',
                          '&:hover': {
                            bgcolor: '#1565c0',
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
                    color: '#a9a7a7ff',
                    backgroundColor: '#2d284a',
                    borderRadius: '12px',
                  },
                  '& .MuiSvgIcon-root': {
                    color: 'white',
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

          {/* Upload File */}
          {/* <Box flex={1}>
            <Typography variant="subtitle1" sx={{ fontWeight: 600, color: '#1976d2', mb: 1 }}>
              Upload File
            </Typography>
            <FormControl fullWidth>
              <Button
                variant="outlined"
                component="label"
                startIcon={<UploadFileIcon sx={{ color: '#FFA500' }} />} // orange icon
                // disabled={!selectedSourceSystem}   
                sx={{
                  padding: '12px 18px',
                  border: '2px dashed #90caf9',
                    backgroundColor: '#2d284a',
                  color: '#FFA500', // bright orange text
                  '& .MuiButton-label': {
                    color: '#FFA500', // ensure label text is orange
                  },
                  '&:hover': {
                    backgroundColor: '#3a3b47',
                    borderColor: '#7cb342',
                  },
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
                      setAvailableFiles((prev) => [
                        ...prev,
                        {
                          name: file.name,
                          type: file.name.includes('_Output') ? 'output' : 'source',
                          displayName: file.name,
                        },
                      ]);
                    }
                  }}
                  accept=".csv,.xlsx,.json"
                />
              </Button>
            </FormControl>

            {selectedFile && (
              <Box
                sx={{
                  width: "50%",           // wider than before for better display
                  mt: 1,
                  p: 1,                  // keep padding small
                  display: 'flex',
                  alignItems: 'center',
                  bgcolor: '#cacbccff', // keep original color
                  borderRadius: 2,            // slightly rounded
                  boxShadow: '0 2px 6px rgba(33, 150, 243, 0.1)',
                  border: '1px solid #90caf9',
                  ml: 'auto',                 // align to right
                }}
              >
                <InsertDriveFileIcon sx={{ fontSize: 28, color: '#527cb4ff', mr: 1.5 }} />
                <Box sx={{ flexGrow: 1 }}>
                  <Typography
                    variant="body2"
                    sx={{
                      fontWeight: 500,
                      whiteSpace: 'nowrap',
                      overflow: 'hidden',
                      textOverflow: 'ellipsis',
                      color: '#1565c0',
                      fontSize: '0.8rem',
                    }}
                    title={selectedFile}
                  >
                    {selectedFile}
                  </Typography>
                </Box>
                <IconButton size="small" sx={{ p: 0.5 }} onClick={() => setSelectedFile('')}>
                  <CloseIcon fontSize="small" />
                </IconButton>
              </Box>
            )}

          </Box> */}

          <Box flex={1}>
            <Typography variant="subtitle1" sx={{ fontWeight: 600, color: '#1976d2', mb: 1 }}>
              Category
            </Typography>
            <FormControl
              fullWidth
              variant="outlined"
              sx={{
                '& .MuiOutlinedInput-root': {
                  borderRadius: '12px',
                  backgroundColor: '#2d284a',
                  boxShadow: '0 2px 6px rgba(0,0,0,0.1)',
                  '& fieldset': { borderColor: '#90caf9' },
                  '&:hover fieldset': { borderColor: '#42a5f5' },
                  '&.Mui-focused fieldset': { borderColor: '#1976d2', borderWidth: '2px' },
                },
                '& .MuiSelect-select': {
                  padding: '14px',
                  fontSize: '1rem',
                  fontWeight: 500,
                  color: '#a9a7a7ff', 
                },
                '& .MuiSvgIcon-root': { color: 'white' },
              }}
            >
              <InputLabel sx={{ fontWeight: 500, fontSize: 12, color: '#acc1d7ff' }}>
                Select category
              </InputLabel>
              <Select
                value={selectedCategory}
                onChange={(e) => setSelectedCategory(e.target.value)}
                label="Select category..."
                MenuProps={{
                  PaperProps: {
                    sx: {
                      borderRadius: 2,
                      boxShadow: '0 4px 12px rgba(0,0,0,0.15)',
                      '& .MuiMenuItem-root': {
                        fontWeight: 500,
                        fontSize: 15,
                        color: '#000000',
                        bgcolor: '#ffffff',
                        '&:hover': {
                          bgcolor: '#e3f2fd',
                          color: '#1976d2',
                        },
                        '&.Mui-selected': {
                          bgcolor: '#1976d2',
                          color: '#ffffff',
                          '&:hover': {
                            bgcolor: '#1565c0',
                          },
                        },
                      },
                    },
                  },
                }}
              >
                <MenuItem value="">
                  <em>Select category...</em>
                </MenuItem>
                {['Header', 'Address', 'Contact'].map((category) => (
                  <MenuItem key={category} value={category}>
                    {category}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
          </Box>


        </Stack>

        {/* Row: Match Rule + Merge Rule */}
        <Stack direction="row" spacing={3} alignItems="flex-start">
          {/* Match Rule */}
          <Box flex={1}>
            <Typography variant="subtitle1" sx={{ fontWeight: 600, color: '#1976d2', mb: 1 }}>
              Match Rule
            </Typography>
            <FormControl
              fullWidth
              variant="outlined"
              sx={{
                '& .MuiOutlinedInput-root': {
                  borderRadius: '12px',
                  backgroundColor: '#2d284a',
                  boxShadow: '0 2px 6px rgba(0,0,0,0.1)',
                  '& fieldset': { borderColor: '#90caf9' },
                  '&:hover fieldset': { borderColor: '#42a5f5' },
                  '&.Mui-focused fieldset': { borderColor: '#1976d2', borderWidth: '2px' },
                },
                '& .MuiSvgIcon-root': {
                  color: 'white',
                },
              }}
            >
              <InputLabel id="match-rule-label" sx={{ fontWeight: 500, fontSize: 12, color: '#acc1d7ff' }}>
                Select match rule
              </InputLabel>
              <Select
                labelId="match-rule-label"
                sx={{ height: '3.3rem' }}
                multiple
                value={selectedRule}
                onChange={(e) =>
                  setSelectedRule(
                    typeof e.target.value === 'string' ? e.target.value.split(',') : e.target.value
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
                    <Typography sx={{ fontWeight: 600, color: '#a9a7a7ff' }}>{selected.join(', ')}</Typography>
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

            {selectedRule.length > 0 && (() => {
              let fuzzyColumns = [], exactColumns = [], thresholds = {};
              selectedRule.forEach((ruleName) => {
                const ruleObj = rules.find((r) => r.rule === ruleName);
                if (ruleObj) {
                  fuzzyColumns = [...fuzzyColumns, ...(ruleObj.fuzzy_columns || [])];
                  exactColumns = [...exactColumns, ...(ruleObj.exact_columns || [])];
                  thresholds = { ...thresholds, ...(ruleObj.thresholds || {}) };
                }
              });
              fuzzyColumns = [...new Set(fuzzyColumns)];
              exactColumns = [...new Set(exactColumns)];
              return <SelectedColumnsDisplay
                mainPage={true}
                fuzzyColumns={fuzzyColumns}
                exactColumns={exactColumns}
                thresholds={thresholds} />;
            })()}
          </Box>

          {/* Merge Rule */}
          <Box flex={1}>
            <MergeRuleSelector
              isCrossSystem={false}
              mergeRule={mergeRule}
              onChange={setMergeRule}
            />

            {mergeRule.length > 0 && (
              <Paper
                sx={{
                  mt: 1.5,
                  elevation: 1,
                  p: 1,             // reduced padding
                  bgcolor: '#f4faff',
                  borderRadius: 1.5, // slightly smaller radius
                  border: '0px solid #ffcc80',
                }}
              >
                <Typography
                  variant="body2"
                  sx={{ fontWeight: 600, color: 'black', mb: 0.5, fontSize: '0.8rem' }}
                >
                  Selected Merge Rule(s):
                </Typography>
                <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
                  {mergeRule.map((rule, index) => (
                    <Box
                      key={index}
                      sx={{
                        display: 'flex',
                        alignItems: 'center',
                        px: 1,
                        m: 1,
                        py: 0.25,
                        border: '1px solid #fb8c00',
                        bgcolor: '#fff8e1',
                        color: 'black',
                        borderRadius: '12px',
                        fontWeight: 500,
                        fontSize: '0.75rem',
                      }}
                    >
                      {rule}
                      <IconButton
                        size="small"
                        sx={{ ml: 0.5, p: 0.25, color: '#a9a7a7ff' }} // smaller spacing and padding
                        onClick={() =>
                          setMergeRule((prev) => prev.filter((r) => r !== rule))
                        }
                      >
                        <CloseIcon fontSize="small" />
                      </IconButton>
                    </Box>
                  ))}
                </Box>
              </Paper>
            )}

          </Box>
        </Stack>

        {/* Process Button */}
        <Box>
          <Button
            variant="contained"
            startIcon={<PlaylistAddCheckIcon />}
            onClick={handleProcess}
            disabled={!selectedSourceSystem || !selectedCategory || selectedRule.length === 0 || mergeRule.length === 0}
            sx={{
              bgcolor: '#123f6cff',      
              color: '#ffffff', 
              '&:hover': {
                bgcolor: '#1565c0',
              },
              '&.Mui-disabled': {
                bgcolor: '#1c3951ff',    // Lighter blue when disabled
                color: '#ffffff',
              },
            }}
          >
            Select Cleanse Rules
          </Button>
        </Box>

      </Stack>
    </Box>
  );
};

export default FileSystemMappingItem;

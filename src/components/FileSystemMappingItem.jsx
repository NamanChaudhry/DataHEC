// // // src/components/FileSystemMappingItem.jsx
// // import React from 'react';
// // import Box from '@mui/material/Box';
// // import Grid from '@mui/material/Grid';
// // import FormControl from '@mui/material/FormControl';
// // import InputLabel from '@mui/material/InputLabel';
// // import Select from '@mui/material/Select';
// // import MenuItem from '@mui/material/MenuItem';
// // import Button from '@mui/material/Button';
// // import AddIcon from '@mui/icons-material/Add';

// // const FileSystemMappingItem = ({
// //   entity,
// //   sourceSystems,
// //   selectedSourceSystem,
// //   setSelectedSourceSystem,
// //   files,
// //   selectedFile,
// //   setSelectedFile,
// //   onAddFile
// // }) => {
// //   return (
// //     <Box>
// //       <Grid container spacing={3}>
// //         <Grid item xs={12} md={4} >
// //           <FormControl fullWidth>
// //             <InputLabel shrink>Source System</InputLabel>
// //             <Select
// //               value={selectedSourceSystem}
// //               label="Source System"
// //               onChange={(e) => setSelectedSourceSystem(e.target.value)}
// //               disabled={!entity}
// //             >
// //               {sourceSystems.map(system => (
// //                 <MenuItem key={system} value={system}>{system}</MenuItem>
// //               ))}
// //             </Select>
// //           </FormControl>
// //         </Grid>
        
// //         <Grid item xs={12} md={4}>
// //           <FormControl fullWidth disabled={!selectedSourceSystem}>
// //             <InputLabel>File</InputLabel>
// //             <Select
// //               value={selectedFile}
// //               label="File"
// //               onChange={(e) => setSelectedFile(e.target.value)}
// //             >
// //               {files.map(file => (
// //                 <MenuItem key={file} value={file}>{file}</MenuItem>
// //               ))}
// //             </Select>
// //           </FormControl>
// //         </Grid>
        
// //         <Grid item xs={12} md={4} sx={{ display: 'flex', alignItems: 'center' }}>
// //           <Button
// //             variant="outlined"
// //             color="primary"
// //             startIcon={<AddIcon />}
// //             onClick={onAddFile}
// //             disabled={!selectedSourceSystem || !selectedFile}
// //             fullWidth
// //           >
// //             Add File Configuration
// //           </Button>
// //         </Grid>
// //       </Grid>
// //     </Box>
// //   );
// // };

// // export default FileSystemMappingItem;

// import React from 'react';
// import Box from '@mui/material/Box';
// import Stack from '@mui/material/Stack';
// import FormControl from '@mui/material/FormControl';
// import InputLabel from '@mui/material/InputLabel';
// import Select from '@mui/material/Select';
// import MenuItem from '@mui/material/MenuItem';
// import Button from '@mui/material/Button';
// import AddIcon from '@mui/icons-material/Add';
// import Typography from '@mui/material/Typography';

// const FileSystemMappingItem = ({
//   entity,
//   sourceSystems,
//   selectedSourceSystem,
//   setSelectedSourceSystem,
//   files,
//   selectedFile,
//   setSelectedFile,
//   onAddFile
// }) => {
//   return (
//     <Box sx={{ width: '100%', overflow: 'visible' }}>
//       <Stack spacing={2}>
//         {/* Source System Row */}
//         <Box>
//           <Typography variant="subtitle2" gutterBottom>
//             Source System
//           </Typography>
//           <FormControl fullWidth>
//             <Select
//               value={selectedSourceSystem}
//               onChange={(e) => setSelectedSourceSystem(e.target.value)}
//               disabled={!entity}
//               displayEmpty
//               sx={{
//                 '& .MuiSelect-select': {
//                   padding: '14px',
//                   fontSize: '1rem',
//                 }
//               }}
//             >
//               <MenuItem value="">
//                 <em>Select source system...</em>
//               </MenuItem>
//               {sourceSystems.map((system) => (
//                 <MenuItem key={system} value={system}>
//                   {system}
//                 </MenuItem>
//               ))}
//             </Select>
//           </FormControl>
//         </Box>

//         {/* File Row */}
//         <Box>
//           <Typography variant="subtitle2" gutterBottom>
//             File (Source or Processed Output)
//           </Typography>
//           <FormControl fullWidth disabled={!selectedSourceSystem}>
//             <Select
//               value={selectedFile}
//               onChange={(e) => setSelectedFile(e.target.value)}
//               displayEmpty
//               sx={{
//                 '& .MuiSelect-select': {
//                   padding: '14px',
//                   fontSize: '1rem',
//                 }
//               }}
//             >
//               <MenuItem value="">
//                 <em>Select file...</em>
//               </MenuItem>
//               {files.map((file) => (
//                 <MenuItem key={file} value={file}>
//                   {file}
//                 </MenuItem>
//               ))}
//             </Select>
//           </FormControl>
//         </Box>

//         {/* Button Row */}
//         <Box sx={{ pt: 1 }}>
//           <Button
//             variant="contained"
//             color="primary"
//             startIcon={<AddIcon />}
//             onClick={onAddFile}
//             disabled={!selectedSourceSystem || !selectedFile}
//             size="large"
//             sx={{
//               width: 'fit-content',
//               minWidth: '200px',
//               padding: '12px 24px',
//               fontSize: '1rem'
//             }}
//           >
//             Add File Configuration
//           </Button>
//         </Box>
//       </Stack>
//     </Box>
//   );
// };

// export default FileSystemMappingItem;

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

  useEffect(() => {
    fetch('/utils/matchrules.json')
      .then((res) => res.json())
      .then((data) => setRules(data))
      .catch((err) => console.error('Error loading match rules:', err));
  }, []);

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
            <InputLabel
              id="match-rule-label"

              sx={{ fontWeight: 500, fontSize: 12, color: '#1976d2' }}
            >Select match rule
            </InputLabel>

            <Select
              labelId="match-rule-label"
              multiple
              value={selectedRule}
              onChange={(e) => setSelectedRule(typeof e.target.value === 'string' ? e.target.value.split(',') : e.target.value)}
              label="Match Rule"
              renderValue={(selected) => selected.join(', ')}
              MenuProps={{
                PaperProps: {
                  sx: {
                    bgcolor: '#ffffff',
                    borderRadius: 2,
                    boxShadow: '0 4px 12px rgba(0,0,0,0.15)',
                    '& .MuiMenuItem-root': {
                      fontWeight: 500,
                      fontSize : '0.5rem',
                      color: '#000000',
                      bgcolor: '#ffffff',
                      '&:hover': {
                        bgcolor: '#e3f2fd',
                        color: '#1976d2',
                      },
                      '&.Mui-selected': {
                        bgcolor: '#2c2f4a', // dark background for selected item only
                        color: '#ffffff',
                        '&:hover': {
                          bgcolor: '#3c3f5c', // hover 
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
                  color: '#0a2958ff',
                  backgroundColor: '#e3f2fd',
                  borderRadius: '12px',
                },
              }}
            >
              {rules.length === 0 ? (
                <MenuItem disabled>
                  <CircularProgress size={20} sx={{ mr: 1 }} /> Loading rules...
                </MenuItem>
              ) : (
                rules.map((rule, index) => (
                  <MenuItem key={index} value={rule}>
                    <Checkbox
                      checked={selectedRule.indexOf(rule) > -1}
                      sx={{
                        color: '#0a1f44', // dark navy blue
                        '&.Mui-checked': {
                          color: '#bbdefb', // same color
                        },
                        '&.MuiCheckbox-root': {
                          '& svg': {
                            fontSize: 20, // size of checkbox icon
                          },
                        },
                      }}
                    />
                    <ListItemText
                      primary={rule}
                      primaryTypographyProps={{ fontSize: '0.9rem' }} // ✅ Smaller text
                    />
                  </MenuItem>
                ))
              )}
            </Select>
          </FormControl>
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
            startIcon={<AddIcon />}
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
            File Display 
          </Button>
        </Box>
      </Stack>
    </Box>
  );
};

export default FileSystemMappingItem;

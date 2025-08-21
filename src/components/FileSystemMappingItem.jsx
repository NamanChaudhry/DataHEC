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
import React from 'react';
import Box from '@mui/material/Box';
import Stack from '@mui/material/Stack';
import FormControl from '@mui/material/FormControl';
import InputLabel from '@mui/material/InputLabel';
import Select from '@mui/material/Select';
import MenuItem from '@mui/material/MenuItem';
import Button from '@mui/material/Button';
import AddIcon from '@mui/icons-material/Add';
import Typography from '@mui/material/Typography';
import Tooltip from '@mui/material/Tooltip';
import Chip from '@mui/material/Chip';

const FileSystemMappingItem = ({
  entity,
  sourceSystems,
  selectedSourceSystem,
  setSelectedSourceSystem,
  files,
  selectedFile,
  setSelectedFile,
  onAddFile
}) => {
  console.log('FileSystemMappingItem rendered with files:', files);
  
  return (
    <Box sx={{ width: '100%', overflow: 'visible' }}>
      <Stack spacing={3}>
        {/* Source System Row */}
        <Box>
          <Typography variant="subtitle2" gutterBottom>
            Source System
          </Typography>
          <FormControl fullWidth>
            <Select
              value={selectedSourceSystem}
              onChange={(e) => setSelectedSourceSystem(e.target.value)}
              disabled={!entity}
              displayEmpty
              sx={{
                '& .MuiSelect-select': {
                  padding: '14px',
                  fontSize: '1rem',
                }
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
          <Typography variant="subtitle2" gutterBottom>
            File (Source or Processed Output) - UPDATED VERSION
          </Typography>
          <FormControl fullWidth disabled={!selectedSourceSystem}>
            <Select
              value={selectedFile}
              onChange={(e) => setSelectedFile(e.target.value)}
              displayEmpty
              sx={{
                '& .MuiSelect-select': {
                  padding: '14px',
                  fontSize: '1rem',
                },
                // Ensure dropdown can show full width
                '& .MuiSelect-menu': {
                  maxHeight: '300px',
                },
                '& .MuiMenuItem-root': {
                  whiteSpace: 'normal',
                  wordBreak: 'break-word',
                  minHeight: 'auto',
                  padding: '12px 16px',
                  lineHeight: '1.4',
                }
              }}
              // Configure dropdown to show full content
              MenuProps={{
                PaperProps: {
                  style: {
                    maxHeight: 300,
                    minWidth: 500, // Made even wider for testing
                    width: 'auto',
                  },
                },
                anchorOrigin: {
                  vertical: 'bottom',
                  horizontal: 'left',
                },
                transformOrigin: {
                  vertical: 'top',
                  horizontal: 'left',
                },
              }}
            >
              <MenuItem value="">
                <em>Select file...</em>
              </MenuItem>
              {files.map((file) => {
                // Handle both string arrays (old format) and object arrays (new format)
                const fileName = typeof file === 'string' ? file : file.name;
                const fileType = typeof file === 'string' ? 
                  (file.includes('_Output') ? 'output' : 'source') : 
                  file.type;
                const displayName = typeof file === 'string' ? file : file.displayName;
                
                return (
                  <MenuItem 
                    key={fileName} 
                    value={fileName}
                    sx={{
                      whiteSpace: 'normal',
                      wordBreak: 'break-word',
                      maxWidth: '600px', // Allow wider menu items
                      minHeight: '60px',
                      display: 'block',
                      padding: '12px 16px',
                      backgroundColor: '#f0f0f0', // Add background color for testing
                      '&:hover': {
                        backgroundColor: '#e0e0e0',
                      }
                    }}
                  >
                    <Tooltip title={displayName} placement="top" arrow>
                      <Box sx={{ 
                        display: 'flex', 
                        flexDirection: 'column',
                        gap: 0.5,
                        width: '100%'
                      }}>
                        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 0.5 }}>
                          <Chip 
                            size="small" 
                            label={fileType} 
                            color={fileType === 'output' ? 'success' : 'default'}
                          />
                        </Box>
                        <Typography 
                          variant="body2" 
                          sx={{ 
                            fontWeight: 500,
                            wordBreak: 'break-all',
                            lineHeight: 1.3,
                            color: '#000'
                          }}
                        >
                          📄 {fileName}
                        </Typography>
                        <Typography 
                          variant="caption" 
                          sx={{ 
                            color: 'blue',
                            fontSize: '0.7rem',
                            fontWeight: 'bold'
                          }}
                        >
                          {fileType === 'output' ? '📊 Processed Output' : '📄 Source File'}
                        </Typography>
                      </Box>
                    </Tooltip>
                  </MenuItem>
                );
              })}
            </Select>
          </FormControl>
          
          {/* Show selected file info */}
          {selectedFile && (
            <Box sx={{ 
              mt: 2, 
              p: 2, 
              bgcolor: '#e8f5e8', 
              borderRadius: 1,
              border: '2px solid #4caf50'
            }}>
              <Typography variant="caption" color="text.secondary">
                ✅ Selected File:
              </Typography>
              <Typography 
                variant="body2" 
                sx={{ 
                  fontWeight: 500,
                  wordBreak: 'break-all',
                  mt: 0.5,
                  color: '#2e7d32'
                }}
              >
                {selectedFile}
              </Typography>
            </Box>
          )}
        </Box>

        {/* Button Row */}
        <Box sx={{ pt: 1 }}>
          <Button
            variant="contained"
            color="primary"
            startIcon={<AddIcon />}
            onClick={onAddFile}
            disabled={!selectedSourceSystem || !selectedFile}
            size="large"
            sx={{
              width: 'fit-content',
              minWidth: '200px',
              padding: '12px 24px',
              fontSize: '1rem'
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

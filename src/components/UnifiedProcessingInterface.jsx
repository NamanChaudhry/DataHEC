
// UnifiedProcessingInterface.jsx
import React, { useState, useEffect, useCallback } from 'react';
import Box from '@mui/material/Box';
import Paper from '@mui/material/Paper';
import Typography from '@mui/material/Typography';
import FormControlLabel from '@mui/material/FormControlLabel';
import Switch from '@mui/material/Switch';
import Button from '@mui/material/Button';
import Divider from '@mui/material/Divider';
import Stack from '@mui/material/Stack';
import Alert from '@mui/material/Alert';
import Grid from '@mui/material/Grid';
import FormControl from '@mui/material/FormControl';
import InputLabel from '@mui/material/InputLabel';
import Select from '@mui/material/Select';
import MenuItem from '@mui/material/MenuItem';
import DeleteIcon from '@mui/icons-material/Delete';
import IconButton from '@mui/material/IconButton';
import Chip from '@mui/material/Chip';
import Collapse from '@mui/material/Collapse';
import axios from 'axios';
import WorkingColumnMapping from './WorkingColumnMapping';
import FileSystemMappingItem from './FileSystemMappingItem'; // ADD THIS IMPORT

import MergeRuleSelector from './Mergeruleselector';


const UnifiedProcessingInterface = ({ entity, onProcess, sourceSystems, columns }) => {
  const [crossSystemEnabled, setCrossSystemEnabled] = useState(false);
  const [fileConfigs, setFileConfigs] = useState([]);
  const [globalFuzzyColumns, setGlobalFuzzyColumns] = useState([]);
  const [globalExactColumns, setGlobalExactColumns] = useState([]);
  const [globalThresholds, setGlobalThresholds] = useState({});
  const [availableColumns, setAvailableColumns] = useState([]);
  const [mergeRule, setMergeRule] = useState('');

  
  // For adding new files
  const [selectedSourceSystem, setSelectedSourceSystem] = useState('');
  const [selectedFile, setSelectedFile] = useState('');
  const [availableFiles, setAvailableFiles] = useState([]);
  const [processedOutputs, setProcessedOutputs] = useState({});
  const [showProcessedOutputs, setShowProcessedOutputs] = useState(false);
  
  // For cross-system auto-selection
  const [crossSystemFileSelections, setCrossSystemFileSelections] = useState({});

  useEffect(() => {
    if (entity) {
      setFileConfigs([]);
      setCrossSystemEnabled(false);
      setGlobalFuzzyColumns([]);
      setGlobalExactColumns([]);
      setGlobalThresholds({});
      setAvailableColumns([]);
      setCrossSystemFileSelections({});
      loadProcessedOutputs();
    }
  }, [entity]);

  useEffect(() => {
    // When cross-system mode is enabled, automatically add all source systems
    if (crossSystemEnabled && entity && sourceSystems.length > 0) {
      autoAddAllSourceSystems();
    } else {
      setFileConfigs([]);
      setCrossSystemFileSelections({});
    }
  }, [crossSystemEnabled, entity, sourceSystems, processedOutputs]);

  useEffect(() => {
    const allColumns = fileConfigs.reduce((acc, config) => [...acc, ...config.columns], []);
    const uniqueColumns = [...new Set(allColumns)];
    setAvailableColumns(uniqueColumns);
  }, [fileConfigs]);

  useEffect(() => {
    if (selectedSourceSystem && entity) {
      loadAvailableFiles();
      setSelectedFile('');
    } else {
      setAvailableFiles([]);
    }
  }, [selectedSourceSystem, entity, processedOutputs]);

  const loadProcessedOutputs = () => {
    axios.get(`http://localhost:5001/api/processed-outputs/${entity}`)
      .then(response => {
        setProcessedOutputs(response.data);
      })
      .catch(err => {
        console.error('Error loading processed outputs:', err);
        setProcessedOutputs({});
      });
  };

  const autoAddAllSourceSystems = () => {
    const newConfigs = [];
    const promises = [];

    sourceSystems.forEach(sourceSystem => {
      // Get source files for this source system
      const promise = axios.get(`http://localhost:5001/api/files/${entity}/${sourceSystem}`)
        .then(response => {
          const sourceFiles = response.data;
          const outputFiles = processedOutputs[sourceSystem] || [];
          
          // Create a config for this source system with all available options
          if (sourceFiles.length > 0 || outputFiles.length > 0) {
            // Default to the first source file, but allow switching
            const defaultFile = sourceFiles[0] || outputFiles[0];
            const defaultType = sourceFiles.length > 0 ? 'source' : 'output';
            
            // Get columns for the default file
            const apiUrl = defaultType === 'source' 
              ? `http://localhost:5001/api/columns/${entity}/${sourceSystem}/${defaultFile}`
              : `http://localhost:5001/api/output-columns/${defaultFile}`;
              
            return axios.get(apiUrl)
              .then(columnsResponse => ({
                id: `${sourceSystem}-${Date.now()}`,
                sourceSystem,
                filename: defaultFile,
                fileType: defaultType,
                displayName: defaultType === 'source' ? defaultFile : `${defaultFile} (Processed Output)`,
                columns: [...columnsResponse.data],
                fuzzyColumns: [],
                exactColumns: [],
                thresholds: {},
                // Available options for this source system
                availableSourceFiles: sourceFiles,
                availableOutputFiles: outputFiles
              }));
          }
          return null;
        })
        .catch(err => {
          console.error(`Error loading files for ${sourceSystem}:`, err);
          return null;
        });
      
      promises.push(promise);
    });

    Promise.all(promises)
      .then(results => {
        const validConfigs = results.filter(config => config !== null);
        setFileConfigs(validConfigs);
        console.log(`Auto-added ${validConfigs.length} source systems for cross-system processing`);
      })
      .catch(error => {
        console.error("Error auto-adding source systems:", error);
      });
  };

  const handleSwitchFileType = (configId, newFileName, newFileType) => {
    // Find the config and update it with new file and columns
    const config = fileConfigs.find(c => c.id === configId);
    if (!config) return;

    const apiUrl = newFileType === 'source' 
      ? `http://localhost:5001/api/columns/${entity}/${config.sourceSystem}/${newFileName}`
      : `http://localhost:5001/api/output-columns/${newFileName}`;

    axios.get(apiUrl)
      .then(columnsResponse => {
        setFileConfigs(prevConfigs =>
          prevConfigs.map(prevConfig =>
            prevConfig.id === configId
              ? {
                  ...prevConfig,
                  filename: newFileName,
                  fileType: newFileType,
                  displayName: newFileType === 'source' ? newFileName : `${newFileName} (Processed Output)`,
                  columns: [...columnsResponse.data],
                  // Reset column selections when switching files
                  fuzzyColumns: [],
                  exactColumns: [],
                  thresholds: {}
                }
              : prevConfig
          )
        );
      })
      .catch(error => {
        console.error("Error switching file type:", error);
        alert("❌ Failed to load columns for selected file");
      });
  };

  const loadAvailableFiles = () => {
    // Get original source files
    axios.get(`http://localhost:5001/api/files/${entity}/${selectedSourceSystem}`)
      .then(sourceFilesResponse => {
        const sourceFiles = sourceFilesResponse.data.map(file => ({
          name: file,
          type: 'source',
          displayName: file
        }));

        // Get processed outputs for this source system
        const outputFiles = processedOutputs[selectedSourceSystem] || [];
        const processedFiles = outputFiles.map(file => ({
          name: file,
          type: 'output',
          displayName: `${file} (Processed Output)`
        }));

        setAvailableFiles([...sourceFiles, ...processedFiles]);
      })
      .catch(err => {
        console.error('Error loading files:', err);
        setAvailableFiles([]);
      });
  };

  const handleAddFile = () => {
    if (!selectedSourceSystem || !selectedFile) {
      alert('Please select a source system and file first');
      return;
    }

    const selectedFileObj = availableFiles.find(f => f.name === selectedFile);
    if (!selectedFileObj) {
      alert('Selected file not found');
      return;
    }

    const exists = fileConfigs.some(
      config => config.sourceSystem === selectedSourceSystem && config.filename === selectedFile
    );
    if (exists) {
      alert(`Configuration for ${selectedSourceSystem}/${selectedFile} already exists`);
      return;
    }

    let apiUrl;
    if (selectedFileObj.type === 'source') {
      apiUrl = `http://localhost:5001/api/columns/${entity}/${selectedSourceSystem}/${selectedFile}`;
    } else {
      apiUrl = `http://localhost:5001/api/output-columns/${selectedFile}`;
    }

    axios.get(apiUrl)
      .then(columnsResponse => {
        if (!columnsResponse.data || columnsResponse.data.length === 0) {
          alert(`No columns found for file: ${selectedFile}`);
          return;
        }

        const newConfig = {
          id: `${selectedSourceSystem}-${selectedFile}-${Date.now()}`,
          sourceSystem: selectedSourceSystem,
          filename: selectedFile,
          fileType: selectedFileObj.type,
          displayName: selectedFileObj.displayName,
          columns: [...columnsResponse.data],
          fuzzyColumns: [],
          exactColumns: [],
          thresholds: {}
        };

        setFileConfigs(prevConfigs => [...prevConfigs, newConfig]);
        setSelectedSourceSystem('');
        setSelectedFile('');
        setAvailableFiles([]);
      })
      .catch(error => {
        console.error("Error adding file:", error);
        alert("Failed to fetch columns. Check backend connection.");
      });
  };

  const handleRemoveConfig = (configId) => {
    setFileConfigs(fileConfigs.filter(config => config.id !== configId));
  };

  const updateConfigMapping = useCallback((configId, newFuzzyColumns, newExactColumns, newThresholds) => {
    setFileConfigs(prevConfigs =>
      prevConfigs.map(config =>
        config.id === configId
          ? {
              ...config,
              fuzzyColumns: [...newFuzzyColumns],
              exactColumns: [...newExactColumns],
              thresholds: { ...newThresholds }
            }
          : config
      )
    );
  }, []);

  const handleSingleFileProcess = (config) => {
    // Basic validation
    if (config.fuzzyColumns.length === 0 && config.exactColumns.length === 0) {
      alert('Please set column mappings before processing');
      return;
    }

    const payload = {
      entity,
      source_system: config.sourceSystem,
      filename: config.filename,
      file_type: config.fileType,
      fuzzy_columns: config.fuzzyColumns,
      exact_columns: config.exactColumns,
      thresholds: config.thresholds
    };

    console.log('Processing single file:', payload);
    
    axios.post('http://localhost:5001/api/process-single', payload)
      .then(response => {
        alert(`✅ File processed successfully! Output: ${response.data.output_file}`);
        // Refresh processed outputs
        loadProcessedOutputs();
      })
      .catch(error => {
        console.error("Error processing file:", error);
        alert("❌ Failed to process file. Check backend connection.");
      });
  };

  const handleUseInCrossSystem = (sourceSystem, outputFile) => {
    // Switch to cross-system mode and add this file
    setCrossSystemEnabled(true);
    
    // Get columns for this output file
    axios.get(`http://localhost:5001/api/output-columns/${outputFile}`)
      .then(columnsResponse => {
        const newConfig = {
          id: `${sourceSystem}-${outputFile}-${Date.now()}`,
          sourceSystem: sourceSystem,
          filename: outputFile,
          fileType: 'output',
          displayName: `${outputFile} (Processed Output)`,
          columns: [...columnsResponse.data],
          fuzzyColumns: [],
          exactColumns: [],
          thresholds: {}
        };

        setFileConfigs(prevConfigs => [...prevConfigs, newConfig]);
        
        // Show success message
        alert(`✅ Added ${outputFile} to cross-system configuration`);
      })
      .catch(error => {
        console.error("Error adding file to cross-system:", error);
        alert("❌ Failed to add file to cross-system mode");
      });
  };

  const handleDeleteSpecificOutput = (sourceSystem, outputFile) => {
    const confirmed = window.confirm(`Are you sure you want to delete ${outputFile}?`);
    if (!confirmed) return;

    axios.delete(`http://localhost:5001/api/clear-specific-output/${entity}/${sourceSystem}/${outputFile}`)
      .then(() => {
        // Refresh processed outputs
        loadProcessedOutputs();
        alert(`✅ Deleted ${outputFile}`);
      })
      .catch(error => {
        console.error('Error deleting output:', error);
        alert('❌ Failed to delete output file');
      });
  };

  const handleCrossSystemProcess = () => {
    console.log('🔄 Starting cross-system processing...');
    console.log('File configs:', fileConfigs);
    console.log('Global fuzzy columns:', globalFuzzyColumns);
    console.log('Global exact columns:', globalExactColumns);

    // Basic validation
    if (fileConfigs.length === 0) {
      alert('Please add at least one file configuration');
      return;
    }

    if (fileConfigs.length < 2) {
      alert('Cross-system deduplication requires at least 2 files');
      return;
    }

    // Check if we have different source systems
    const uniqueSourceSystems = [...new Set(fileConfigs.map(config => config.sourceSystem))];
    if (uniqueSourceSystems.length < 2) {
      alert('Cross-system deduplication requires files from at least 2 different source systems');
      return;
    }

    if (globalFuzzyColumns.length === 0 && globalExactColumns.length === 0) {
      alert('Please set global column mappings for cross-system deduplication');
      return;
    }

    // Build payload - for cross-system, we don't need individual file column configs
    const payload = {
      entity,
      file_configs: fileConfigs.map(config => ({
        source_system: config.sourceSystem,
        filename: config.filename,
        file_type: config.fileType,
        // No individual column configs for cross-system mode
        fuzzy_columns: [],
        exact_columns: [],
        thresholds: {}
      })),
      global_fuzzy_columns: globalFuzzyColumns,
      global_exact_columns: globalExactColumns,
      global_thresholds: globalThresholds
    };

    console.log('Cross-system payload:', JSON.stringify(payload, null, 2));
    onProcess(payload, true);
  };

  return (
    <Box sx={{ width: '100%', mt: 4 }}>
      <Paper elevation={3} sx={{ p: 3, borderRadius: 2 }}>
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
          <Typography variant="h5">
            Unified Processing Interface
            {crossSystemEnabled ? 
              <span style={{ color: '#1976d2', fontSize: '0.8em' }}> (Cross-System Mode)</span> : 
              <span style={{ color: '#ed6c02', fontSize: '0.8em' }}> (Single File Mode)</span>
            }
          </Typography>
          <FormControlLabel
            control={
              <Switch
                checked={crossSystemEnabled}
                onChange={(e) => setCrossSystemEnabled(e.target.checked)}
                color="primary"
              />
            }
            label="Cross-System Mode"
          />
        </Box>

        <Alert severity="info" sx={{ mb: 3 }}>
          {crossSystemEnabled
            ? "Cross-system mode: Select multiple files (source files or processed outputs) from different source systems for global deduplication across all files."
            : "Single file mode: Process individual files and generate outputs that can be used later in cross-system mode."}
        </Alert>

        {/* REPLACE THE INLINE FILE SELECTION WITH FileSystemMappingItem COMPONENT */}
        {!crossSystemEnabled && (
          <Paper elevation={1} sx={{ p: 2, mb: 3, bgcolor: '#f9f9f9' }}>
            <Typography variant="h6" gutterBottom>File Display</Typography>
            <FileSystemMappingItem
              entity={entity}
              sourceSystems={sourceSystems}
              selectedSourceSystem={selectedSourceSystem}
              setSelectedSourceSystem={setSelectedSourceSystem}
              files={availableFiles}
              selectedFile={selectedFile}
              setSelectedFile={setSelectedFile}
              onAddFile={handleAddFile}
            />
          </Paper>
        )}
                <MergeRuleSelector
  isCrossSystem={crossSystemEnabled}
  mergeRule={mergeRule}
  onChange={setMergeRule}
/>

        {/* Cross-System Auto-Added Files */}
        {crossSystemEnabled && fileConfigs.length > 0 && (
          <Paper elevation={1} sx={{ p: 2, mb: 3, bgcolor: '#f0f7ff' }}>
            <Typography variant="h6" gutterBottom sx={{ color: '#1976d2' }}>
              📋 Cross-System Files (Auto-Added)
            </Typography>
            <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
              All source systems have been automatically added. Choose between source files or processed outputs for each system.
            </Typography>
            
            <Grid container spacing={2}>
              {fileConfigs.map((config) => (
                <Grid item xs={12} md={4} key={config.id}>
                  <Paper elevation={2} sx={{ p: 2, bgcolor: '#fff', border: '1px solid #e3f2fd' }}>
                    <Typography variant="subtitle1" fontWeight="bold" gutterBottom>
                      {config.sourceSystem}
                    </Typography>
                    
                    {/* Source Files Selection */}
                    {config.availableSourceFiles && config.availableSourceFiles.length > 0 && (
                      <Box sx={{ mb: 2 }}>
                        <Typography variant="body2" fontWeight="medium" color="text.secondary" sx={{ mb: 1 }}>
                          Source Files:
                        </Typography>
                        <FormControl fullWidth size="small">
                          <Select
                            value={config.fileType === 'source' ? config.filename : ''}
                            onChange={(e) => handleSwitchFileType(config.id, e.target.value, 'source')}
                            displayEmpty
                          >
                            <MenuItem value="">
                              <em>Select source file</em>
                            </MenuItem>
                            {config.availableSourceFiles.map(file => (
                              <MenuItem key={file} value={file}>
                                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                                  <Chip size="small" label="source" color="default" />
                                  {file}
                                </Box>
                              </MenuItem>
                            ))}
                          </Select>
                        </FormControl>
                      </Box>
                    )}
                    
                    {/* Processed Output Files Selection */}
                    {config.availableOutputFiles && config.availableOutputFiles.length > 0 && (
                      <Box sx={{ mb: 2 }}>
                        <Typography variant="body2" fontWeight="medium" color="text.secondary" sx={{ mb: 1 }}>
                          Processed Outputs:
                        </Typography>
                        <FormControl fullWidth size="small">
                          <Select
                            value={config.fileType === 'output' ? config.filename : ''}
                            onChange={(e) => handleSwitchFileType(config.id, e.target.value, 'output')}
                            displayEmpty
                          >
                            <MenuItem value="">
                              <em>Select processed output</em>
                            </MenuItem>
                            {config.availableOutputFiles.map(file => (
                              <MenuItem key={file} value={file}>
                                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                                  <Chip size="small" label="output" color="success" />
                                  {file}
                                </Box>
                              </MenuItem>
                            ))}
                          </Select>
                        </FormControl>
                      </Box>
                    )}
                    
                    {/* Currently Selected */}
                    <Box sx={{ p: 1, bgcolor: '#f5f5f5', borderRadius: 1 }}>
                      <Typography variant="caption" color="text.secondary">
                        Currently Selected:
                      </Typography>
                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mt: 0.5 }}>
                        <Chip 
                          size="small" 
                          label={config.fileType} 
                          color={config.fileType === 'output' ? 'success' : 'default'}
                        />
                        <Typography variant="body2" fontWeight="medium">
                          {config.filename}
                        </Typography>
                      </Box>
                      <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mt: 0.5 }}>
                        Columns: {config.columns.length}
                      </Typography>
                    </Box>
                  </Paper>
                </Grid>
              ))}
            </Grid>
          </Paper>
        )}

        {/* Processed Outputs Display Section */}
        {!crossSystemEnabled && Object.keys(processedOutputs).length > 0 && (
          <>
            <Divider sx={{ my: 3 }} />
            <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
              <Typography variant="h5">
                Processed Outputs for {entity}
              </Typography>
              <Button
                variant="outlined"
                size="small"
                onClick={() => setShowProcessedOutputs(!showProcessedOutputs)}
              >
                {showProcessedOutputs ? 'Hide' : 'Show'} Outputs
              </Button>
            </Box>
            
            <Collapse in={showProcessedOutputs}>
              <Stack spacing={2}>
                {Object.entries(processedOutputs).map(([sourceSystem, outputs]) => (
                  <Paper key={sourceSystem} elevation={1} sx={{ p: 2, bgcolor: '#f8f9fa' }}>
                    <Typography variant="h6" color="primary" gutterBottom>
                      {sourceSystem} ({outputs.length} files)
                    </Typography>
                    <Grid container spacing={1}>
                      {outputs.map((outputFile, index) => (
                        <Grid item xs={12} sm={6} md={4} key={index}>
                          <Paper 
                            elevation={1} 
                            sx={{ 
                              p: 1.5, 
                              bgcolor: '#fff',
                              border: '1px solid #e0e0e0',
                              '&:hover': { boxShadow: 2 }
                            }}
                          >
                            <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
                              <Typography variant="body2" fontWeight="medium">
                                📄 {outputFile}
                              </Typography>
                              <Box sx={{ display: 'flex', gap: 1 }}>
                                <Button
                                  size="small"
                                  variant="outlined"
                                  href={`http://localhost:5001/api/download/${outputFile}`}
                                  target="_blank"
                                  sx={{ fontSize: '0.7rem', py: 0.5 }}
                                >
                                  📥 Download
                                </Button>
                                <Button
                                  size="small"
                                  variant="outlined"
                                  color="success"
                                  onClick={() => handleUseInCrossSystem(sourceSystem, outputFile)}
                                  sx={{ fontSize: '0.7rem', py: 0.5 }}
                                >
                                  ➕ Add to Cross-System
                                </Button>
                                <Button
                                  size="small"
                                  variant="outlined"
                                  color="error"
                                  onClick={() => handleDeleteSpecificOutput(sourceSystem, outputFile)}
                                  sx={{ fontSize: '0.7rem', py: 0.5 }}
                                >
                                  🗑️ Delete
                                </Button>
                              </Box>
                            </Box>
                          </Paper>
                        </Grid>
                      ))}
                    </Grid>
                  </Paper>
                ))}
              </Stack>
            </Collapse>
          </>
        )}

        <Divider sx={{ my: 3 }} />

        {/* File Configurations */}
        {fileConfigs.length > 0 && (
          <>
            <Typography variant="h5" gutterBottom>
              Selected File Configurations ({fileConfigs.length})
            </Typography>
            <Stack spacing={3} sx={{ mb: 3 }}>
              {fileConfigs.map((config) => (
                <Paper key={config.id} elevation={2} sx={{ p: 2, borderRadius: 2 }}>
                  <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
                    <Box>
                      <Typography variant="h6">
                        {config.sourceSystem} / {config.filename}
                      </Typography>
                      <Box sx={{ display: 'flex', gap: 1, mt: 1 }}>
                        <Chip 
                          size="small" 
                          label={config.fileType} 
                          color={config.fileType === 'output' ? 'success' : 'default'}
                        />
                        <Chip 
                          size="small" 
                          label={`Fuzzy: ${config.fuzzyColumns.length}`}
                          variant="outlined"
                        />
                        <Chip 
                          size="small" 
                          label={`Exact: ${config.exactColumns.length}`}
                          variant="outlined"
                        />
                      </Box>
                    </Box>
                    <Box sx={{ display: 'flex', gap: 1 }}>
                      {!crossSystemEnabled && (
                        <Button
                          variant="contained"
                          size="small"
                          color="success"
                          onClick={() => handleSingleFileProcess(config)}
                          disabled={config.fuzzyColumns.length === 0 && config.exactColumns.length === 0}
                        >
                          Process
                        </Button>
                      )}
                      <IconButton
                        color="error"
                        onClick={() => handleRemoveConfig(config.id)}
                        aria-label="delete configuration"
                      >
                        <DeleteIcon />
                      </IconButton>
                    </Box>
                  </Box>

                  {!crossSystemEnabled && (
                    <WorkingColumnMapping
                      columns={config.columns}
                      fuzzyColumns={config.fuzzyColumns}
                      exactColumns={config.exactColumns}
                      thresholds={config.thresholds}
                      onMappingChange={(newFuzzy, newExact, newThresholds) => {
                        updateConfigMapping(config.id, newFuzzy, newExact, newThresholds);
                      }}
                    />
                  )}
                  
                  {crossSystemEnabled && (
                    <Box sx={{ p: 2, bgcolor: '#f0f7ff', borderRadius: 1 }}>
                      <Typography variant="body2" color="text.secondary">
                        📋 Available columns: {config.columns.join(', ')}
                      </Typography>
                      <Typography variant="caption" color="primary">
                        File type can be changed above. Column configuration will be set globally for all files.
                      </Typography>
                    </Box>
                  )}
                </Paper>
              ))}
            </Stack>
          </>
        )}

        {/* Global Cross-System Settings */}
        {crossSystemEnabled && availableColumns.length > 0 && (
          <Paper elevation={3} sx={{ p: 3, borderRadius: 2, mb: 3, bgcolor: '#f5f5f5' }}>
            <Typography variant="h5" gutterBottom sx={{ color: '#1976d2' }}>
              Global Cross-System Settings
            </Typography>
            <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
              These settings will be used for the final cross-system deduplication across all selected files.
              Available columns: {availableColumns.join(', ')}
            </Typography>

            <WorkingColumnMapping
              columns={availableColumns}
              fuzzyColumns={globalFuzzyColumns}
              exactColumns={globalExactColumns}
              thresholds={globalThresholds}
              onMappingChange={(newFuzzy, newExact, newThresholds) => {
                setGlobalFuzzyColumns(newFuzzy);
                setGlobalExactColumns(newExact);
                setGlobalThresholds(newThresholds);
              }}
            />
          </Paper>
        )}

        {/* Process Button */}
        {crossSystemEnabled && fileConfigs.length > 0 && (
          <Box sx={{ display: 'flex', justifyContent: 'center', mt: 3 }}>
            <Button
              variant="contained"
              color="primary"
              size="large"
              onClick={handleCrossSystemProcess}
              startIcon="🔄"
            >
              Process {fileConfigs.length} Files with Cross-System Deduplication
            </Button>
          </Box>
        )}

        {!crossSystemEnabled && fileConfigs.length > 0 && (
          <Alert severity="success" sx={{ mt: 3 }}>
            <Typography variant="body2">
              In single file mode, configure fuzzy/exact columns for each file and process them individually using the "Process" button. 
              Processed outputs will be available for cross-system mode.
            </Typography>
          </Alert>
        )}

        {crossSystemEnabled && fileConfigs.length > 0 && fileConfigs.length < 2 && (
          <Alert severity="warning" sx={{ mt: 3 }}>
            <Typography variant="body2">
              Cross-system mode requires at least 2 files from different source systems. 
              Add more files to enable cross-system processing.
            </Typography>
          </Alert>
        )}

        {crossSystemEnabled && fileConfigs.length >= 2 && availableColumns.length === 0 && (
          <Alert severity="info" sx={{ mt: 3 }}>
            <Typography variant="body2">
              Set the global fuzzy/exact column configuration below to process all {fileConfigs.length} files together.
              Individual file column configurations are not needed in cross-system mode.
            </Typography>
          </Alert>
        )}
      </Paper>
    </Box>
  );
};

export default UnifiedProcessingInterface;
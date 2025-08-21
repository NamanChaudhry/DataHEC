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
import FileSystemMappingItem from './FileSystemMappingItem';
import WorkingColumnMapping from './WorkingColumnMapping';
import Grid from '@mui/material/Grid';
import DeleteIcon from '@mui/icons-material/Delete';
import IconButton from '@mui/material/IconButton';
import axios from 'axios';

const CrossSystemSelector = ({ entity, onProcess, sourceSystems, columns }) => {
  const [crossSystemEnabled, setCrossSystemEnabled] = useState(false);
  const [fileConfigs, setFileConfigs] = useState([]);
  const [globalFuzzyColumns, setGlobalFuzzyColumns] = useState([]);
  const [globalExactColumns, setGlobalExactColumns] = useState([]);
  const [globalThresholds, setGlobalThresholds] = useState({});
  const [availableColumns, setAvailableColumns] = useState([]);
  const [selectedSourceSystem, setSelectedSourceSystem] = useState('');
  const [selectedFile, setSelectedFile] = useState('');
  const [files, setFiles] = useState([]);

  useEffect(() => {
    if (entity) {
      setFileConfigs([]);
      setCrossSystemEnabled(false);
      setGlobalFuzzyColumns([]);
      setGlobalExactColumns([]);
      setGlobalThresholds({});
      setAvailableColumns([]);
    }
  }, [entity]);

  useEffect(() => {
    const allColumns = fileConfigs.reduce((acc, config) => [...acc, ...config.columns], []);
    const uniqueColumns = [...new Set(allColumns)];
    setAvailableColumns(uniqueColumns);
  }, [fileConfigs]);

  useEffect(() => {
    if (selectedSourceSystem && entity) {
      axios.get(`http://localhost:5001/api/files/${entity}/${selectedSourceSystem}`)
        .then(res => setFiles(res.data))
        .catch(err => {
          console.error(err);
          setFiles([]);
        });
      setSelectedFile('');
    } else {
      setFiles([]);
    }
  }, [selectedSourceSystem, entity]);

  const handleAddFile = async () => {
    if (!selectedSourceSystem || !selectedFile) {
      alert('Please select a source system and file first');
      return;
    }

    const exists = fileConfigs.some(
      config => config.sourceSystem === selectedSourceSystem && config.filename === selectedFile
    );
    if (exists) {
      alert(`Configuration for ${selectedSourceSystem}/${selectedFile} already exists`);
      return;
    }

    try {
      const response = await axios.get(
        `http://localhost:5001/api/columns/${entity}/${selectedSourceSystem}/${selectedFile}`
      );

      if (!response.data || response.data.length === 0) {
        alert(`No columns found for file: ${selectedFile}`);
        return;
      }

      const newConfig = {
        id: `${selectedSourceSystem}-${selectedFile}-${Date.now()}`,
        sourceSystem: selectedSourceSystem,
        filename: selectedFile,
        columns: [...response.data],
        fuzzyColumns: [],
        exactColumns: [],
        thresholds: {}
      };

      setFileConfigs(prevConfigs => [...prevConfigs, newConfig]);
      setSelectedSourceSystem('');
      setSelectedFile('');
      setFiles([]);
    } catch (error) {
      console.error("Error adding file:", error);
      alert("Failed to fetch columns. Check backend connection.");
    }
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

  const handleProcess = () => {
    console.log('🔄 Starting processing...');
    console.log('Cross-system enabled:', crossSystemEnabled);
    console.log('File configs:', fileConfigs);
    console.log('Global fuzzy columns:', globalFuzzyColumns);
    console.log('Global exact columns:', globalExactColumns);

    // Basic validation
    if (fileConfigs.length === 0) {
      alert('Please add at least one file configuration');
      return;
    }

    // Check that all files have column mappings
    const invalidConfigs = fileConfigs.filter(
      config => config.fuzzyColumns.length === 0 && config.exactColumns.length === 0
    );

    if (invalidConfigs.length > 0) {
      const invalidFiles = invalidConfigs.map(config => `${config.sourceSystem}/${config.filename}`).join(', ');
      alert(`Please set column mappings for all files. Missing mappings for: ${invalidFiles}`);
      return;
    }

    // Validation specific to cross-system mode
    if (crossSystemEnabled) {
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
    }

    // Build payload
    const payload = {
      entity,
      file_configs: fileConfigs.map(config => ({
        source_system: config.sourceSystem,
        filename: config.filename,
        fuzzy_columns: config.fuzzyColumns,
        exact_columns: config.exactColumns,
        thresholds: config.thresholds
      }))
    };

    // Add global settings only if cross-system is enabled
    if (crossSystemEnabled) {
      payload.global_fuzzy_columns = globalFuzzyColumns;
      payload.global_exact_columns = globalExactColumns;
      payload.global_thresholds = globalThresholds;
    } else {
      // For independent processing, explicitly set empty arrays
      payload.global_fuzzy_columns = [];
      payload.global_exact_columns = [];
      payload.global_thresholds = {};
    }

    console.log('Final payload being sent:', JSON.stringify(payload, null, 2));
    onProcess(payload, crossSystemEnabled);
  };

  return (
    <Box sx={{ width: '100%', mt: 4 }}>
      <Paper elevation={3} sx={{ p: 3, borderRadius: 2 }}>
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
          <Typography variant="h5">
            Multi-File Processing 
            {crossSystemEnabled ? 
              <span style={{ color: '#1976d2', fontSize: '0.8em' }}> (Cross-System Mode)</span> : 
              <span style={{ color: '#ed6c02', fontSize: '0.8em' }}> (Independent Mode)</span>
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
            label="Enable Cross-System Deduplication"
          />
        </Box>

        <Alert severity="info" sx={{ mb: 3 }}>
          {crossSystemEnabled
            ? "Cross-system mode: Select multiple files from different source systems. Files will be processed individually, then cross-system deduplication will find global winners across all files."
            : "Independent processing mode: Process multiple files independently. Each file will be deduplicated separately with no cross-system comparison."}
        </Alert>

        <FileSystemMappingItem
          entity={entity}
          sourceSystems={sourceSystems}
          selectedSourceSystem={selectedSourceSystem}
          setSelectedSourceSystem={setSelectedSourceSystem}
          files={files}
          selectedFile={selectedFile}
          setSelectedFile={setSelectedFile}
          onAddFile={handleAddFile}
        />

        <Divider sx={{ my: 3 }} />

        {fileConfigs.length > 0 && (
          <>
            <Typography variant="h5" gutterBottom>
              Selected File Configurations ({fileConfigs.length})
            </Typography>
            <Stack spacing={3} sx={{ mb: 3 }}>
              {fileConfigs.map((config) => (
                <Paper key={config.id} elevation={2} sx={{ p: 2, borderRadius: 2 }}>
                  <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
                    <Typography variant="h6">
                      {config.sourceSystem} / {config.filename}
                    </Typography>
                    <Box>
                      <Typography variant="caption" color="text.secondary" sx={{ mr: 2 }}>
                        Fuzzy: {config.fuzzyColumns.length}, Exact: {config.exactColumns.length}
                      </Typography>
                      <IconButton
                        color="error"
                        onClick={() => handleRemoveConfig(config.id)}
                        aria-label="delete configuration"
                      >
                        <DeleteIcon />
                      </IconButton>
                    </Box>
                  </Box>

                  <WorkingColumnMapping
                    columns={config.columns}
                    fuzzyColumns={config.fuzzyColumns}
                    exactColumns={config.exactColumns}
                    thresholds={config.thresholds}
                    onMappingChange={(newFuzzy, newExact, newThresholds) => {
                      updateConfigMapping(config.id, newFuzzy, newExact, newThresholds);
                    }}
                  />
                </Paper>
              ))}
            </Stack>
          </>
        )}

        {crossSystemEnabled && availableColumns.length > 0 && (
          <Grid item xs={12}>
            <Paper elevation={3} sx={{ p: 3, borderRadius: 2, mb: 3, bgcolor: '#f5f5f5' }}>
              <Typography variant="h5" gutterBottom sx={{ color: '#1976d2' }}>
                Global Cross-System Settings
              </Typography>
              <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
                These settings will be used when performing the final cross-system deduplication across all files.
                Columns available: {availableColumns.join(', ')}
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
          </Grid>
        )}

        {fileConfigs.length > 0 && (
          <Box sx={{ display: 'flex', justifyContent: 'center', mt: 3 }}>
            <Button
              variant="contained"
              color="primary"
              size="large"
              onClick={handleProcess}
              startIcon={crossSystemEnabled ? "🔄" : "📁"}
            >
              {crossSystemEnabled
                ? `Process ${fileConfigs.length} Files + Cross-System Deduplication`
                : `Process ${fileConfigs.length} Files Independently`}
            </Button>
          </Box>
        )}
      </Paper>
    </Box>
  );
};

export default CrossSystemSelector;
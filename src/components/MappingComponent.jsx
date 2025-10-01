import React, { useState } from 'react';
import Box from '@mui/material/Box';
import Paper from '@mui/material/Paper';
import Typography from '@mui/material/Typography';
import Alert from '@mui/material/Alert';
import Button from '@mui/material/Button';
import MappingInputs from './MappingInputs';

const MappingComponent = ({ entity, sourceSystems }) => {
  const [fileConfigs, setFileConfigs] = useState([]);
  const [showMappingText, setShowMappingText] = useState({}); // {configId: true/false}

  const [selectedSourceSystem, setSelectedSourceSystem] = useState('');
  const [selectedFile, setSelectedFile] = useState('');
  const [availableFiles, setAvailableFiles] = useState([]);

  const handleAddFileConfig = (newConfig) => {
    const id = `${newConfig.sourceSystem}-${Date.now()}`;
    setFileConfigs(prev => [...prev, { ...newConfig, id, displayName: newConfig.filename || 'Unnamed File' }]);
  };

  const handleShowMapping = (config) => {
    // Check conditions: source system and file must be selected
    if (!config.sourceSystem || !config.filename) {
      alert('Please select a source system and upload a file before mapping.');
      return;
    }

    setShowMappingText(prev => ({ ...prev, [config.id]: true }));
  };

  return (
    <Box sx={{ width: '100%', mt: 2 }}>
      <Paper elevation={3} sx={{ p: 2, borderRadius: 2, background: "#fdfbfbff" }}>
        <Box sx={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          mb: 1,
          p: 1.4,
          borderRadius: 2,
          background: 'linear-gradient(135deg, #0d3965 0%, #205988 100%)'
        }}>
          <Typography variant="h6" sx={{ color: '#cfccccff' }}>
            Unified Processing Interface
            <span style={{ color: '#ed6c02', fontSize: '0.8em' }}> (Single File Mode)</span>
          </Typography>
        </Box>

        <Alert severity="info" sx={{ mb: 3, mt: 2, border: '1px solid #37474f' }}>
          Single file mode: Process individual files and generate outputs that can be used later in cross-system mode.
        </Alert>

        <Paper elevation={0} sx={{ p: 2, mb: 3, background: "#f7f7f7ff", border: '1px solid #cfd5deff', boxShadow: '0 2px 10px rgba(0, 0, 0, 0.04)' }}>
          <Typography variant="h6" gutterBottom sx={{ color: '#1976d2' }}>File Display</Typography>
          <MappingInputs
            entity={entity}
            sourceSystems={sourceSystems}
            selectedSourceSystem={selectedSourceSystem}
            setSelectedSourceSystem={setSelectedSourceSystem}
            files={availableFiles}
            selectedFile={selectedFile}
            setSelectedFile={setSelectedFile}
            availableFiles={availableFiles}
            setAvailableFiles={setAvailableFiles}
            onAddFile={(ruleConfig) => handleAddFileConfig(ruleConfig)}
          />
        </Paper>

      </Paper>
    </Box>
  );
};

export default MappingComponent;

import React, { useEffect, useState } from 'react';
import Box from '@mui/material/Box';
import Container from '@mui/material/Container';
import Paper from '@mui/material/Paper';
import Typography from '@mui/material/Typography';
import Alert from '@mui/material/Alert';
import Collapse from '@mui/material/Collapse';
import CircularProgress from '@mui/material/CircularProgress';
import Button from '@mui/material/Button';
import Grid from '@mui/material/Grid';
import EntitySelector from '../components/EntitySelector';
import axios from 'axios';  
import UnifiedProcessingInterface from '../components/UnifiedProcessingInterface';
import MappingComponent from '../components/MappingComponent';

const MappingPage = ({ setDownloads }) => {
  const [entities, setEntities] = useState([]);
  const [selectedEntity, setSelectedEntity] = useState('');
  const [sourceSystems, setSourceSystems] = useState([]);
  const [loading, setLoading] = useState(false);
  const [resultMessage, setResultMessage] = useState(null);
  const [resultDetails, setResultDetails] = useState([]);
  const [resultType, setResultType] = useState('success');
  const [processingStats, setProcessingStats] = useState(null);
  const [processingStartTime, setProcessingStartTime] = useState(null);


  useEffect(() => {
    console.log('[DEBUG] Fetching entities from backend...');

    axios.get('http://localhost:5001/api/entities')
      .then(res => {
        console.log('[DEBUG] Response received from backend:', res);
        console.log('[DEBUG] Entity list:', res.data);
        setEntities(res.data);
      })
      .catch(err => {
        console.error('[ERROR] Failed to load entities:', err);
      });
  }, []);

  useEffect(() => {
    if (selectedEntity) {
      axios.get(`http://localhost:5001/api/source-systems/${selectedEntity}`)
        .then(res => setSourceSystems(res.data))
        .catch(err => console.error('Error loading source systems:', err));

      // Clear previous results
      setResultMessage(null);
      setResultDetails([]);
    }
  }, [selectedEntity]);

  const handleProcess = async (payload, isCrossSystem) => {
    const startTime = Date.now();
    setLoading(true);
    setResultMessage(null);
    setResultDetails([]);
    setProcessingStats(null);
    setProcessingStartTime(startTime);

    try {
      console.log('Processing started at:', new Date(startTime).toLocaleTimeString());
      console.log('Processing payload:', payload);

      const endpoint = isCrossSystem
        ? 'http://localhost:5001/api/process-cross-system'
        : 'http://localhost:5001/api/process-single';

      const response = await axios.post(endpoint, payload);

      const endTime = Date.now();
      const totalTime = endTime - startTime;

      console.log('Processing completed at:', new Date(endTime).toLocaleTimeString());
      console.log('Total frontend time:', totalTime, 'ms');

      let files = [];

      if (response.data.outputs && Array.isArray(response.data.outputs)) {
        files = response.data.outputs;
      } else if (response.data.output_file) {
        files = [response.data.output_file];
      }

      if (files.length > 0) {
        setResultMessage(`✅ Processing complete. ${files.length} file${files.length > 1 ? 's' : ''} generated.`);
        setResultType('success');
        setResultDetails(files);
        setDownloads(files);
      } else {
        setResultMessage("⚠️ Processing completed but no output files were generated.");
        setResultType('warning');
        setResultDetails([]);
      }

      const stats = {
        frontend_time: totalTime,
        backend_time: response.data.processing_time_ms || null,
        total_records: response.data.total_records || null,
        duplicate_groups: response.data.duplicate_groups || null,
        final_records: response.data.final_records || null,
        duplicates_found: response.data.duplicates_found || null,
        fuzzy_columns: response.data.fuzzy_columns || payload.fuzzy_columns || payload.global_fuzzy_columns || [],
        exact_columns: response.data.exact_columns || payload.exact_columns || payload.global_exact_columns || [],
        files_processed: isCrossSystem ? payload.file_configs?.length : 1,
        file_size_mb: response.data.total_file_size_mb || response.data.file_size_mb || null,
        memory_used_mb: response.data.memory_used_mb || null,
        performance_stats: response.data.performance_stats || null
      };
      setProcessingStats(stats);

    } catch (error) {
      const endTime = Date.now();
      const totalTime = endTime - startTime;

      console.error('[ERROR] Processing failed');
      console.error('[ERROR] Full error object:', error);

      let errorMsg = "❌ Processing failed. Check console for details.";

      // More detailed backend error message
      if (error.response) {
        console.error('[ERROR] Status:', error.response.status);
        console.error('[ERROR] Backend response:', error.response.data);

        if (error.response.data.message) {
          errorMsg = `❌ ${error.response.data.message}`;
        } else if (typeof error.response.data === 'string') {
          errorMsg = `❌ ${error.response.data}`;
        }
      }

      // 🛠️ Check if files were still returned in the error response
      let files = [];

      if (error.response && Array.isArray(error.response.data?.outputs)) {
        files = error.response.data.outputs;
      } else if (error.response?.data?.output_file) {
        files = [error.response.data.output_file];
      }

      if (files.length > 0) {
        setResultMessage(`✅ Processing complete with warnings. ${files.length} file${files.length > 1 ? 's' : ''} generated.`);
        setResultType('warning');
        setResultDetails(files);
        setDownloads(files);
      } else {
        setResultMessage(errorMsg);
        setResultType('error');
      }

      setProcessingStats({
        frontend_time: totalTime,
        backend_time: null,
        error: true
      });

    } finally {
      setLoading(false);
      setProcessingStartTime(null);
    }
  };


  const handleClearProcessedOutputs = async () => {
    if (!selectedEntity) return;

    const confirmed = window.confirm(
      `Are you sure you want to delete ALL processed output files for ${selectedEntity}? This action cannot be undone.`
    );

    if (!confirmed) return;

    try {
      const response = await axios.delete(`http://localhost:5001/api/clear-processed-outputs/${selectedEntity}`);
      setResultMessage(`✅ ${response.data.message}`);
      setResultType('success');
      setResultDetails(response.data.deleted_files || []);

      // Refresh the page data
      window.location.reload();

    } catch (error) {
      console.error('Error clearing processed outputs:', error);
      setResultMessage('❌ Failed to clear processed outputs');
      setResultType('error');
    }
  };

  const handleRefreshData = () => {
    window.location.reload();
  };

  return (
    <Box
      component="main"
      sx={{
        mt: 1,
        ml: '0rem',
        mr: 0,
        px: 0,
        width: '100%',
        maxWidth: '100%',
        background: "transparent",
        boxSizing: "border-box",
        display: "flex",
        flexDirection: "column",
        alignItems: "center", 
        justifyContent: "flex-start",
      }}
    >

      <Paper
        elevation={3}
        sx={{
          width: '100%',
          height: '100%',
          p: { xs: 2, sm: 4, md: 5 },
          borderRadius: 5,
          background: 'linear-gradient(135deg, #e3f2fd 0%, #bbdefb 100%)',
          color: "#100808",
          boxSizing: "border-box",
          display: "flex",
          flexDirection: "column",
          alignItems: "center",
          boxShadow: 3,
          overflowY: "auto",
        }}
      >
        <Typography variant="h4" align="center" gutterBottom sx={{ color: 'black', fontFamily: 'sans-serif' }}>
          EY Data Harmonization
        </Typography>
        <Typography variant="subtitle1" align="center" color="text.secondary" sx={{ mb: 4, color: 'black', fontFamily: 'sans-serif' }}>
          Streamlining Data Harmonization
        </Typography>

        {/* Entity Selection*/}
        <Box sx={{ mb: 4 }}>
          <EntitySelector
            entities={entities}
            selectedEntity={selectedEntity}
            onSelect={setSelectedEntity}
          />
        </Box>

        {/* Main Processing Interface */}
        {selectedEntity && (
          <>
            <MappingComponent
              entity={selectedEntity}
              sourceSystems={sourceSystems}
              onProcess={handleProcess}
            />
          </>
        )}
      </Paper>
    </Box>
  );
};

export default MappingPage;


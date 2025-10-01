// MainContent.jsx
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
import EntitySelector from './EntitySelector';
import UnifiedProcessingInterface from './UnifiedProcessingInterface';
import axios from 'axios';

const MainContent = ({ setDownloads }) => {
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
        ml: '13rem',
        mr: 0,
        px: 0,
        width: '83%',
        maxWidth: '100%',
        background: "transparent",
        boxSizing: "border-box",
        display: "flex",
        flexDirection: "column",
        alignItems: "center", // You can change this to "stretch" if needed
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

        {/* Loading Indicator with Timer */}
        {loading && (
          <Box display="flex" flexDirection="column" alignItems="center" my={4}>
            <CircularProgress size={60} />
            <Box sx={{ mt: 2, textAlign: 'center' }}>
              <Typography variant="h6">Processing files...</Typography>
              {processingStartTime && (
                <Typography variant="body2" color="text.secondary">
                  Elapsed: {Math.floor((Date.now() - processingStartTime) / 1000)}s
                </Typography>
              )}
              <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mt: 1 }}>
                Processing time depends on file size, number of records, and column complexity
              </Typography>
            </Box>
          </Box>
        )}

        <Collapse in={!!resultMessage || resultDetails.length > 0}>
          <Alert
            severity={resultType}
            icon={false}
            sx={{ mb: 4 }}
            onClose={() => {
              setResultMessage(null);
              setResultDetails([]);
              setProcessingStats(null);
            }}
          >
            {resultDetails.length > 0 ? (
              <>
                <Typography variant="subtitle2">Generated Files:</Typography>
                <Box component="ul" sx={{ mt: 1, pl: 2 }}>
                  {resultDetails.map((file, index) => (
                    <li key={index}>
                      <a
                        href={`http://localhost:5001/api/download/${file}`}
                        target="_blank"
                        rel="noopener noreferrer"
                        style={{
                          color: '#1976d2',
                          textDecoration: 'none',
                          fontWeight: 500,
                        }}
                      >
                        📄 {file}
                      </a>
                    </li>
                  ))}
                </Box>
              </>
            ) : (
              <Typography variant="body1">{resultMessage}</Typography>
            )}
          </Alert>
        </Collapse>

        {/* Main Processing Interface */}
        {selectedEntity && (
          <>
            <UnifiedProcessingInterface
              entity={selectedEntity}
              sourceSystems={sourceSystems}
              onProcess={handleProcess}
            />

            {/* Utility Actions */}
            <Box sx={{ mt: 4, display: 'flex', justifyContent: 'center', gap: 2, flexWrap: 'wrap' }}>
              <Button
                variant="outlined"
                color="info"
                onClick={handleRefreshData}
                size="small"
              >
                🔄 Refresh Data
              </Button>
              <Button
                variant="outlined"
                color="warning"
                onClick={handleClearProcessedOutputs}
                size="small"
              >
                🗑️ Clear All Outputs for {selectedEntity}
              </Button>
              <Button
                variant="outlined"
                color="info"
                onClick={() => window.open('http://localhost:5001/api/health', '_blank')}
                size="small"
              >
                ⚡ System Health
              </Button>
            </Box>
          </>
        )}
      </Paper>
    </Box>
  );
};

export default MainContent;



{/* Results Section */ }
{/* <Collapse in={!!resultMessage}>
          <Alert
            severity={resultType}
            sx={{ mb: 4 }}
            onClose={() => {
              setResultMessage(null);
              setProcessingStats(null);
            }}
            action={
              resultDetails.length > 0 && (
                <Button color="inherit" size="small">
                  View Downloads
                </Button>
              )
            }
          >
            <Typography variant="body1">{resultMessage}</Typography> */}

{/* Processing Statistics */ }
{/* {processingStats && (
              <Box sx={{ mt: 2, p: 2, bgcolor: 'rgba(0,0,0,0.05)', borderRadius: 1 }}>
                <Typography variant="subtitle2" gutterBottom>⏱️ Processing Statistics</Typography>
                <Grid container spacing={2}>
                  <Grid item xs={6} sm={3}>
                    <Typography variant="caption" color="text.secondary">Frontend Time</Typography>
                    <Typography variant="body2" fontWeight="medium">
                      {(processingStats.frontend_time / 1000).toFixed(2)}s
                    </Typography>
                  </Grid>
                  {processingStats.backend_time && (
                    <Grid item xs={6} sm={3}>
                      <Typography variant="caption" color="text.secondary">Backend Time</Typography>
                      <Typography variant="body2" fontWeight="medium">
                        {(processingStats.backend_time / 1000).toFixed(2)}s
                      </Typography>
                    </Grid>
                  )}
                  {processingStats.total_records && (
                    <Grid item xs={6} sm={3}>
                      <Typography variant="caption" color="text.secondary">Total Records</Typography>
                      <Typography variant="body2" fontWeight="medium">
                        {processingStats.total_records.toLocaleString()}
                      </Typography>
                    </Grid>
                  )}
                  {processingStats.final_records && (
                    <Grid item xs={6} sm={3}>
                      <Typography variant="caption" color="text.secondary">Final Records</Typography>
                      <Typography variant="body2" fontWeight="medium">
                        {processingStats.final_records.toLocaleString()}
                      </Typography>
                    </Grid>
                  )}
                  {processingStats.duplicate_groups && (
                    <Grid item xs={6} sm={3}>
                      <Typography variant="caption" color="text.secondary">Duplicate Groups</Typography>
                      <Typography variant="body2" fontWeight="medium">
                        {processingStats.duplicate_groups}
                      </Typography>
                    </Grid>
                  )}
                  {processingStats.duplicates_found && (
                    <Grid item xs={6} sm={3}>
                      <Typography variant="caption" color="text.secondary">Duplicates Found</Typography>
                      <Typography variant="body2" fontWeight="medium">
                        {processingStats.duplicates_found.toLocaleString()}
                      </Typography>
                    </Grid>
                  )}
                  <Grid item xs={6} sm={3}>
                    <Typography variant="caption" color="text.secondary">Files Processed</Typography>
                    <Typography variant="body2" fontWeight="medium">
                      {processingStats.files_processed}
                    </Typography>
                  </Grid>
                  {processingStats.file_size_mb && (
                    <Grid item xs={6} sm={3}>
                      <Typography variant="caption" color="text.secondary">File Size</Typography>
                      <Typography variant="body2" fontWeight="medium">
                        {processingStats.file_size_mb.toFixed(2)} MB
                      </Typography>
                    </Grid>
                  )}
                  {processingStats.memory_used_mb && (
                    <Grid item xs={6} sm={3}>
                      <Typography variant="caption" color="text.secondary">Memory Used</Typography>
                      <Typography variant="body2" fontWeight="medium">
                        {processingStats.memory_used_mb.toFixed(2)} MB
                      </Typography>
                    </Grid>
                  )}
                  <Grid item xs={12}>
                    <Typography variant="caption" color="text.secondary">Column Configuration</Typography>
                    <Typography variant="body2">
                      Fuzzy: {Array.isArray(processingStats.fuzzy_columns) ? processingStats.fuzzy_columns.length : 0} columns ({Array.isArray(processingStats.fuzzy_columns) && processingStats.fuzzy_columns.length > 0 ? processingStats.fuzzy_columns.join(', ') : 'none'})
                    </Typography>
                    <Typography variant="body2">
                      Exact: {Array.isArray(processingStats.exact_columns) ? processingStats.exact_columns.length : 0} columns ({Array.isArray(processingStats.exact_columns) && processingStats.exact_columns.length > 0 ? processingStats.exact_columns.join(', ') : 'none'})
                    </Typography>
                  </Grid>
                  {processingStats.performance_stats && (
                    <Grid item xs={12}>
                      <Typography variant="caption" color="text.secondary">Performance Metrics</Typography>
                      <Typography variant="body2" fontWeight="medium">
                        {processingStats.performance_stats.records_per_second} records/second
                        {processingStats.performance_stats.mb_per_second &&
                          ` • ${processingStats.performance_stats.mb_per_second} MB/second`}
                      </Typography>
                    </Grid>
                  )}
                </Grid>
              </Box>
            )}

            {resultDetails.length > 0 && (
              <Box sx={{ mt: 2 }}>
                <Typography variant="subtitle2">Generated Files:</Typography>
                <Box component="ul" sx={{ mt: 1, pl: 2 }}>
                  {resultDetails.map((file, index) => (
                    <li key={index}>
                      <a
                        href={`http://localhost:5001/api/download/${file}`}
                        target="_blank"
                        rel="noopener noreferrer"
                        style={{
                          color: resultType === 'success' ? '#1976d2' : 'inherit',
                          textDecoration: 'none',
                          fontWeight: 500
                        }}
                      >
                        📄 {file}
                      </a>
                    </li>
                  ))}
                </Box>
              </Box>
            )}
          </Alert>
        </Collapse> */}

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
      
      setResultMessage(response.data.message);
      setResultType('success');
      
      // Set processing statistics
      const stats = {
        frontend_time: totalTime,
        backend_time: response.data.processing_time_ms || null,
        total_records: response.data.total_records || null,
        duplicate_groups: response.data.duplicate_groups || null,
        final_records: response.data.final_records || null,
        duplicates_found: response.data.duplicates_found || null,
        fuzzy_columns: payload.fuzzy_columns || payload.global_fuzzy_columns || [],
        exact_columns: payload.exact_columns || payload.global_exact_columns || [],
        files_processed: isCrossSystem ? payload.file_configs?.length : 1,
        file_size_mb: response.data.total_file_size_mb || response.data.file_size_mb || null,
        memory_used_mb: response.data.memory_used_mb || null,
        performance_stats: response.data.performance_stats || null
      };
      setProcessingStats(stats);
      
      if (response.data.outputs) {
        setResultDetails(response.data.outputs);
        setDownloads(response.data.outputs);
      } else if (response.data.output_file) {
        setResultDetails([response.data.output_file]);
        setDownloads([response.data.output_file]);
      }
      
    } catch (error) {
      const endTime = Date.now();
      const totalTime = endTime - startTime;
      
      console.error("Processing error:", error);
      console.log('Processing failed after:', totalTime, 'ms');
      
      setResultMessage("❌ Processing failed. Check console for details.");
      setResultType('error');
      setProcessingStats({
        frontend_time: totalTime,
        backend_time: null,
        error: true
      });
      
      if (error.response && error.response.data) {
        console.error('Server error:', error.response.data);
      }
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
        flexGrow: 1,
        display: "flex",
        justifyContent: "flex-start",
        alignItems: "flex-start",
        minHeight: "100vh",
        background: "#2c2c3a", // keep your dark color
        px: { xs: 0, sm: 2, md: 4 }, // horizontal padding for space from sidebar/right
        ml: { xs: 0, sm: '240px' },
      }}
    >
      <Container
        maxWidth="md"
        sx={{
          mx: "auto",
          my: 4,
          px: { xs: 1, sm: 2, md: 3 },
          width: "100%",
        }}
      >
        <Paper
          elevation={3}
          sx={{
            p: { xs: 0, sm: 4, md: 4 },
            borderRadius: 4,
            background: "#f0f7ff", // keep your dark color
            color: "#100808ff",
            boxSizing: "border-box",
          }}
        >
          <Typography variant="h3" align="center" gutterBottom>
            DataHEC
          </Typography>
          <Typography variant="subtitle1" align="center" color="text.secondary" sx={{ mb: 4 }}>
            DataHEC: Streamlining Data Harmonization and Cleaning Processes
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

          {/* Results Section */}
          <Collapse in={!!resultMessage}>
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
              <Typography variant="body1">{resultMessage}</Typography>

              {/* Processing Statistics */}
              {processingStats && (
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
                        Fuzzy: {processingStats.fuzzy_columns.length} columns ({processingStats.fuzzy_columns.join(', ') || 'none'})
                      </Typography>
                      <Typography variant="body2">
                        Exact: {processingStats.exact_columns.length} columns ({processingStats.exact_columns.join(', ') || 'none'})
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

          {/* Help Section */}
          {/* {!selectedEntity && (
            <Alert severity="info" sx={{ mt: 4 }}>
              <Typography variant="h6" gutterBottom>How to Use This Interface</Typography>
              <Box component="ol" sx={{ pl: 2 }}>
                <li><strong>Single File Mode:</strong> Process individual files and generate outputs that can be reused later</li>
                <li><strong>Cross-System Mode:</strong> Select multiple files (original sources or processed outputs) for global deduplication</li>
                <li><strong>Workflow:</strong> Process files individually first, then use those outputs in cross-system mode for comprehensive deduplication</li>
              </Box>
            </Alert>
          )} */}
        </Paper>
      </Container>
    </Box>
  );
};

export default MainContent;



// // MainContent.jsx
// import React, { useEffect, useState } from 'react';
// import Box from '@mui/material/Box';
// import Container from '@mui/material/Container';
// import Paper from '@mui/material/Paper';
// import Typography from '@mui/material/Typography';
// import Alert from '@mui/material/Alert';
// import Collapse from '@mui/material/Collapse';
// import CircularProgress from '@mui/material/CircularProgress';
// import Button from '@mui/material/Button';
// import Grid from '@mui/material/Grid';
// import EntitySelector from './EntitySelector';
// import UnifiedProcessingInterface from './UnifiedProcessingInterface';
// import axios from 'axios';

// const MainContent = ({ setDownloads }) => {
//   const [entities, setEntities] = useState([]);
//   const [selectedEntity, setSelectedEntity] = useState('');
//   const [sourceSystems, setSourceSystems] = useState([]);
//   const [loading, setLoading] = useState(false);
//   const [resultMessage, setResultMessage] = useState(null);
//   const [resultDetails, setResultDetails] = useState([]);
//   const [resultType, setResultType] = useState('success');
//   const [processingStats, setProcessingStats] = useState(null);
//   const [processingStartTime, setProcessingStartTime] = useState(null);

//   useEffect(() => {
//     axios.get('http://localhost:5000/api/entities')
//       .then(res => setEntities(res.data))
//       .catch(err => console.error('Error loading entities:', err));
//   }, []);

//   useEffect(() => {
//     if (selectedEntity) {
//       axios.get(`http://localhost:5000/api/source-systems/${selectedEntity}`)
//         .then(res => setSourceSystems(res.data))
//         .catch(err => console.error('Error loading source systems:', err));
//       setResultMessage(null);
//       setResultDetails([]);
//     }
//   }, [selectedEntity]);

//   const handleProcess = async (payload, isCrossSystem) => {
//     const startTime = Date.now();
//     setLoading(true);
//     setResultMessage(null);
//     setResultDetails([]);
//     setProcessingStats(null);
//     setProcessingStartTime(startTime);

//     try {
//       const endpoint = isCrossSystem
//         ? 'http://localhost:5000/api/process-multiple'
//         : 'http://localhost:5000/api/process';

//       const response = await axios.post(endpoint, payload);
//       const endTime = Date.now();
//       const totalTime = endTime - startTime;

//       setResultMessage(response.data.message);
//       setResultType('success');

//       const stats = {
//         frontend_time: totalTime,
//         backend_time: response.data.processing_time_ms || null,
//         total_records: response.data.total_records || null,
//         duplicate_groups: response.data.duplicate_groups || null,
//         final_records: response.data.final_records || null,
//         duplicates_found: response.data.duplicates_found || null,
//         fuzzy_columns: payload.fuzzy_columns || payload.global_fuzzy_columns || [],
//         exact_columns: payload.exact_columns || payload.global_exact_columns || [],
//         files_processed: isCrossSystem ? payload.file_configs?.length : 1,
//         file_size_mb: response.data.total_file_size_mb || response.data.file_size_mb || null,
//         memory_used_mb: response.data.memory_used_mb || null,
//         performance_stats: response.data.performance_stats || null
//       };
//       setProcessingStats(stats);

//       if (response.data.outputs) {
//         setResultDetails(response.data.outputs);
//         setDownloads(response.data.outputs);
//       } else if (response.data.output_file) {
//         setResultDetails([response.data.output_file]);
//         setDownloads([response.data.output_file]);
//       }

//     } catch (error) {
//       const endTime = Date.now();
//       const totalTime = endTime - startTime;
//       console.error("Processing error:", error);

//       setResultMessage("\u274C Processing failed. Check console for details.");
//       setResultType('error');
//       setProcessingStats({
//         frontend_time: totalTime,
//         backend_time: null,
//         error: true
//       });

//       if (error.response && error.response.data) {
//         console.error('Server error:', error.response.data);
//       }
//     } finally {
//       setLoading(false);
//       setProcessingStartTime(null);
//     }
//   };

//   const handleClearProcessedOutputs = async () => {
//     if (!selectedEntity) return;
//     const confirmed = window.confirm(`Are you sure you want to delete ALL processed output files for ${selectedEntity}? This action cannot be undone.`);
//     if (!confirmed) return;

//     try {
//       const response = await axios.delete(`http://localhost:5000/api/clear-processed-outputs/${selectedEntity}`);
//       setResultMessage(`\u2705 ${response.data.message}`);
//       setResultType('success');
//       setResultDetails(response.data.deleted_files || []);
//       window.location.reload();
//     } catch (error) {
//       console.error('Error clearing processed outputs:', error);
//       setResultMessage('\u274C Failed to clear processed outputs');
//       setResultType('error');
//     }
//   };

//   const handleRefreshData = () => {
//     window.location.reload();
//   };

//   return (
//     <Box component="main" sx={{ flexGrow: 1, p: 3 }}>
//       <Container maxWidth="lg">
//         <Paper elevation={3} sx={{ p: 4, borderRadius: 4 }}>
//           <Typography variant="h3" align="center" gutterBottom>
//             EY Deduplication Engine
//           </Typography>
//           <Typography variant="subtitle1" align="center" color="text.secondary" sx={{ mb: 4 }}>
//             Unified Processing Interface - Single File & Cross-System Modes
//           </Typography>

//           <Box sx={{ mb: 4 }}>
//             <EntitySelector
//               entities={entities}
//               selectedEntity={selectedEntity}
//               onSelect={setSelectedEntity}
//             />
//           </Box>

//           {loading && (
//             <Box display="flex" flexDirection="column" alignItems="center" my={4}>
//               <CircularProgress size={60} />
//               <Box sx={{ mt: 2, textAlign: 'center' }}>
//                 <Typography variant="h6">Processing files...</Typography>
//                 {processingStartTime && (
//                   <Typography variant="body2" color="text.secondary">
//                     Elapsed: {Math.floor((Date.now() - processingStartTime) / 1000)}s
//                   </Typography>
//                 )}
//               </Box>
//             </Box>
//           )}

//           <Collapse in={!!resultMessage}>
//             <Alert
//               severity={resultType}
//               sx={{ mb: 4 }}
//               onClose={() => {
//                 setResultMessage(null);
//                 setProcessingStats(null);
//               }}
//             >
//               <Typography variant="body1">{resultMessage}</Typography>
//               {resultDetails.length > 0 && (
//                 <ul style={{ marginTop: 10 }}>
//                   {resultDetails.map((file, i) => (
//                     <li key={i}>
//                       <a
//                         href={`http://localhost:5000/api/download/${file}`}
//                         target="_blank"
//                         rel="noopener noreferrer"
//                       >
//                         📄 {file}
//                       </a>
//                     </li>
//                   ))}
//                 </ul>
//               )}
//             </Alert>
//           </Collapse>

//           {selectedEntity && (
//             <>
//               <UnifiedProcessingInterface
//                 entity={selectedEntity}
//                 sourceSystems={sourceSystems}
//                 onProcess={handleProcess}
//               />
//               <Box sx={{ mt: 4, display: 'flex', justifyContent: 'center', gap: 2 }}>
//                 <Button variant="outlined" onClick={handleRefreshData}>🔄 Refresh</Button>
//                 <Button variant="outlined" color="error" onClick={handleClearProcessedOutputs}>🗑️ Clear All Outputs</Button>
//               </Box>
//             </>
//           )}
//         </Paper>
//       </Container>
//     </Box>
//   );
// };

// export default MainContent;

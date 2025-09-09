import React from "react";
import Divider from '@mui/material/Divider';
import Typography from '@mui/material/Typography';
import Stack from '@mui/material/Stack';
import Paper from '@mui/material/Paper';
import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import IconButton from '@mui/material/IconButton';
import Chip from '@mui/material/Chip';
import DeleteIcon from '@mui/icons-material/Delete';
import WorkingColumnMapping from './WorkingColumnMapping';

const ProcessPage = ({
  entity,
  fileConfigs = [],            // default to empty array
  crossSystemEnabled = false,   // default boolean
  updateConfigMapping = () => {}, // default no-op functions
  handleSingleFileProcess = () => {},
  handleRemoveConfig = () => {}
}) => {
  return (
    <div>

      {fileConfigs.length > 0 ? (
        <>
          <Typography variant="h5" gutterBottom>
            Selected File Configurations ({fileConfigs.length})
          </Typography>
          <Stack spacing={3} sx={{ mb: 3 }}>
            {fileConfigs.map((config) => {
              const fuzzyLength = config.fuzzyColumns?.length || 0;
              const exactLength = config.exactColumns?.length || 0;
              const columnsList = config.columns || [];

              return (
                <Paper key={config.id} elevation={2} sx={{ p: 2, borderRadius: 2 }}>
                  <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
                    <Box>
                      <Typography variant="h6">
                        {config.sourceSystem || 'Unknown'} / {config.filename || 'Unknown File'}
                      </Typography>
                      <Box sx={{ display: 'flex', gap: 1, mt: 1 }}>
                        <Chip
                          size="small"
                          label={config.fileType || 'N/A'}
                          color={config.fileType === 'output' ? 'success' : 'default'}
                        />
                        <Chip
                          size="small"
                          label={`Fuzzy: ${fuzzyLength}`}
                          variant="outlined"
                        />
                        <Chip
                          size="small"
                          label={`Exact: ${exactLength}`}
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
                          disabled={fuzzyLength === 0 && exactLength === 0}
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
                      columns={columnsList}
                      fuzzyColumns={config.fuzzyColumns || []}
                      exactColumns={config.exactColumns || []}
                      thresholds={config.thresholds || {}}
                      onMappingChange={(newFuzzy, newExact, newThresholds) => {
                        updateConfigMapping(config.id, newFuzzy, newExact, newThresholds);
                      }}
                    />
                  )}

                  {crossSystemEnabled && (
                    <Box sx={{ p: 2, bgcolor: '#f0f7ff', borderRadius: 1 }}>
                      <Typography variant="body2" color="text.secondary">
                        📋 Available columns: {columnsList.join(', ')}
                      </Typography>
                      <Typography variant="caption" color="primary">
                        File type can be changed above. Column configuration will be set globally for all files.
                      </Typography>
                    </Box>
                  )}
                </Paper>
              );
            })}
          </Stack>
        </>
      ) : (
        <Typography variant="body2" color="text.secondary">
          No file configurations added yet.
        </Typography>
      )}
    </div>
  );
};

export default ProcessPage;

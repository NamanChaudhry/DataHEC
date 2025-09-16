import React from 'react';
import { Box, Paper, Typography, Chip, TextField } from '@mui/material';

const SelectedColumnsDisplay = ({
  fuzzyColumns = [],
  exactColumns = [],
  thresholds = {},
  handleFuzzyToggle = () => {},
  handleExactToggle = () => {},
  handleThresholdChange = () => {},
}) => (
  (fuzzyColumns.length > 0 || exactColumns.length > 0) && (
    <Box sx={{ mb: 2, mt : 2}}>
      {fuzzyColumns.length > 0 && (
        <Paper elevation={1} sx={{ p: 1.3, mb: 1, backgroundColor: '#fffdf5' }}>
          <Typography variant="subtitle2" sx={{ mb: 1, fontWeight: 600 }}>
            Selected Fuzzy Columns:
          </Typography>
          <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1 }}>
            {fuzzyColumns.map(col => (
              <Chip
                key={`fuzzy-chip-${col}`}
                label={
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                    <span style={{ fontSize: '12px' }}>Fuzzy: {col}</span>
                    <TextField
                      size="small"
                      value={thresholds[col] || 90}
                      onChange={(e) => handleThresholdChange(col, e.target.value)}
                      onClick={(e) => e.stopPropagation()}
                      sx={{
                        width: 50,
                        '& input': {
                          padding: '4px',
                          textAlign: 'center',
                          fontSize: '12px'
                        }
                      }}
                    />
                  </Box>
                }
                onDelete={() => handleFuzzyToggle(col)}
                sx={{ bgcolor: '#fff8e1', borderColor: '#ffcd00' }}
                variant="outlined"
              />
            ))}
          </Box>
        </Paper>
      )}

      {exactColumns.length > 0 && (
        <Paper elevation={1} sx={{ p: 1.3, mb: 2, backgroundColor: '#f4faff' }}>
          <Typography variant="subtitle2" sx={{ mb: 1, fontWeight: 600 }}>
            Selected Exact Columns:
          </Typography>
          <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1 }}>
            {exactColumns.map(col => (
              <Chip
                key={`exact-chip-${col}`}
                label={`Exact: ${col}`}
                onDelete={() => handleExactToggle(col)}
                sx={{ bgcolor: '#e3f2fd', fontSize: '12px' }}
              />
            ))}
          </Box>
        </Paper>
      )}
    </Box>
  )
);

export default SelectedColumnsDisplay;
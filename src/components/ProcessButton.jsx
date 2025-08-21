// src/components/ProcessButton.jsx
import React from 'react';
import Button from '@mui/material/Button';
import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';

const ProcessButton = ({ onClick, mode = 'single' }) => {
  const buttonText = mode === 'cross' 
    ? '🔄 Process Cross-System Deduplication' 
    : '🚀 Process File';
  
  const description = mode === 'cross'
    ? 'Process files across different source systems and determine global winners'
    : 'Process the selected file for duplicates';
    
  return (
    <Box sx={{ textAlign: 'center' }}>
      <Button 
        variant="contained" 
        color="primary" 
        size="large" 
        onClick={onClick}
        sx={{ 
          py: 1.5,
          px: 4,
          fontSize: '1.1rem',
          boxShadow: 3
        }}
      >
        {buttonText}
      </Button>
      <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mt: 1 }}>
        {description}
      </Typography>
    </Box>
  );
};

export default ProcessButton;
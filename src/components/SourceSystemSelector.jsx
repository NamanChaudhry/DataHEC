// src/components/SourceSystemSelector.jsx
import React from 'react';
import Typography from '@mui/material/Typography';
import FormControl from '@mui/material/FormControl';
import InputLabel from '@mui/material/InputLabel';
import Select from '@mui/material/Select';
import MenuItem from '@mui/material/MenuItem';

const SourceSystemSelector = ({ sourceSystems, selectedSourceSystem, onSelect }) => (
  <div>
    <Typography variant="h5" gutterBottom>Select Source System</Typography>
    <FormControl fullWidth>
      <InputLabel>Source System</InputLabel>
      <Select value={selectedSourceSystem} label="Source System" onChange={(e) => onSelect(e.target.value)}>
        {sourceSystems.map(system => (
          <MenuItem key={system} value={system}>{system}</MenuItem>
        ))}
      </Select>
    </FormControl>
  </div>
);

export default SourceSystemSelector;
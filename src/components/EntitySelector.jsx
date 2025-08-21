  // src/components/EntitySelector.jsx
  import React from 'react';
  import Typography from '@mui/material/Typography';
  import FormControl from '@mui/material/FormControl';
  import InputLabel from '@mui/material/InputLabel';
  import Select from '@mui/material/Select';
  import MenuItem from '@mui/material/MenuItem';

  const EntitySelector = ({ entities, selectedEntity, onSelect }) => (
    <div>
      <Typography variant="h5" gutterBottom>Select Entity</Typography>
      <FormControl fullWidth>
        <InputLabel>Entity</InputLabel>
        <Select value={selectedEntity} label="Entity" onChange={(e) => onSelect(e.target.value)}>
          {entities.map(entity => (
            <MenuItem key={entity} value={entity}>{entity}</MenuItem>
          ))}
        </Select>
      </FormControl>
    </div>
  );

  export default EntitySelector;
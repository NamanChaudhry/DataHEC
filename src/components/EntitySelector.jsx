import React from 'react';
import Typography from '@mui/material/Typography';
import FormControl from '@mui/material/FormControl';
import InputLabel from '@mui/material/InputLabel';
import Select from '@mui/material/Select';
import MenuItem from '@mui/material/MenuItem';
import { styled } from '@mui/material/styles';

const CustomSelect = styled(Select)(({ theme }) => ({
  backgroundColor: '#263238',   // dropdown background
  color: '#ffffff',             // text color
  height: 40,                   // control height to remove gaps
  padding: '0px 0px',          // inner padding
  '& .MuiSelect-icon': {
    color: '#90caf9',
  },
  '& .MuiOutlinedInput-notchedOutline': {
    borderColor: '#90caf9',
  },
  '&:hover .MuiOutlinedInput-notchedOutline': {
    borderColor: '#64b5f6',
  },
}));

const CustomMenuItem = styled(MenuItem)(({ theme }) => ({
  backgroundColor: '#263238',
  color: '#ffffff',
  '&:hover': {
    backgroundColor: '#37474f',
  },
  '&.Mui-selected': {
    backgroundColor: '#90caf9',
    color: '#263238',
    '&:hover': {
      backgroundColor: '#64b5f6',
    },
  },
}));

const EntitySelector = ({ entities, selectedEntity, onSelect }) => (
  <div>
    <Typography variant="h5" gutterBottom sx={{ color: '#cfccccff' }}>
      Select Entity
    </Typography>
    <FormControl fullWidth sx={{ mt:1 }}>
      <InputLabel sx={{ color: '#90caf9', top: -8, left:5 }}>Entity</InputLabel>
      <CustomSelect
  value={selectedEntity}
  label="Entity"
  onChange={(e) => onSelect(e.target.value)}
  MenuProps={{
    PaperProps: {
      sx: {
        bgcolor: '#263238',
        paddingTop: 0,
        paddingBottom: 0,
        '& .MuiMenuItem-root': {
          paddingTop: 1,
          paddingBottom: 1,
        },
      },
    },
  }}
>
        {entities.map(entity => (
          <CustomMenuItem key={entity} value={entity}>
            {entity}
          </CustomMenuItem>
        ))}
      </CustomSelect>
    </FormControl>
  </div>
);

export default EntitySelector;

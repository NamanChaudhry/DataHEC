// import React from 'react';
// import { FormControl, InputLabel, Select, MenuItem } from '@mui/material';

// const MergeRuleSelector = ({ isCrossSystem, mergeRule, onChange }) => {
//   const handleChange = (event) => {
//     onChange(event.target.value);
//   };

//   return (
//     <FormControl fullWidth style={{ marginTop: 24 }}>
//       <InputLabel id="merge-rule-label">Merge Rule</InputLabel>
//       <Select
//         labelId="merge-rule-label"
//         value={mergeRule}
//         label="Merge Rule"
//         onChange={handleChange}
//       >
//         {isCrossSystem ? (
//           <MenuItem value="source-system-precedence">
//             Source System Precedence (PS91 → PS92 → Medtox)
//           </MenuItem>
//         ) : (
//           <MenuItem value="transaction-date">
//             Transaction Date Precedence
//           </MenuItem>
//         )}
//       </Select>
//     </FormControl>
//   );
// };

// export default MergeRuleSelector;

import React from 'react';
import {
  Box,
  Typography,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Checkbox,
  ListItemText
} from '@mui/material';

const MergeRuleSelector = ({ isCrossSystem, mergeRule, onChange }) => {
  const handleChange = (event) => {
    const {
      target: { value },
    } = event;
    onChange(typeof value === 'string' ? value.split(',') : value);
  };

  const crossSystemOptions = ['Source System Precedence (PS91 → PS92 → Medtox)'];
  const singleFileOptions = [
    'Latest Transaction Date',
    'Earliest Transaction Date',
    'Largest Name'
  ];

  const options = isCrossSystem ? crossSystemOptions : singleFileOptions;

  return (
    <Box>
      <Typography variant="subtitle1" sx={{ fontWeight: 600, color: 'whitesmoke', mb: 1 }}>
        🔀 Merge Rule
      </Typography>
      <FormControl fullWidth variant="outlined"
        sx={{
          '& .MuiOutlinedInput-root': {
            borderRadius: '12px',
            backgroundColor: '#272733',
            boxShadow: '0 2px 6px rgba(0,0,0,0.1)',
            '& fieldset': { borderColor: '#90caf9' },
            '&:hover fieldset': { borderColor: '#42a5f5' },
            '&.Mui-focused fieldset': { borderColor: '#1976d2', borderWidth: '2px' },
          },
        }}
      >
        <InputLabel id="merge-rule-label" sx={{ fontWeight: 500, fontSize: 12, color: '#acc1d7ff' }}>
          Select merge rule
        </InputLabel>
        <Select
          labelId="merge-rule-label"
          multiple
          value={mergeRule}
          onChange={handleChange}
          label="Merge Rule"
          renderValue={(selected) => (
            <Typography sx={{ color: '#a9a7a7ff', fontWeight: 600 }}>
              {selected.join(', ')}
            </Typography>
          )}
          MenuProps={{
            PaperProps: {
              sx: {
                bgcolor: '#272733',
                borderRadius: 2,
                boxShadow: '0 4px 12px rgba(0,0,0,0.15)',
                '& .MuiMenuItem-root': {
                  fontWeight: 500,
                  fontSize: '0.85rem', // ✅ Smaller font size
                  color: '#000000',
                  bgcolor: '#ffffff',
                  '&:hover': {
                    bgcolor: '#e3f2fd',
                    color: '#1976d2',
                  },
                  '&.Mui-selected': {
                    bgcolor: '#2c2f4a',
                    color: '#ffffff',
                    '&:hover': {
                      bgcolor: '#3c3f5c',
                    },
                  },
                },
              },
            },
          }}
          sx={{
            '& .MuiSelect-select': {
              padding: '14px',
              fontSize: '1rem',
              fontWeight: 500,
              color: '#0a2958ff',
              backgroundColor: '#272733',
              borderRadius: '12px',
            },
          }}
        >
          {options.map((option, index) => (
            <MenuItem key={index} value={option}>
              <Checkbox
                checked={mergeRule.indexOf(option) > -1}
                sx={{
                  color: '#0a1f44',
                  '&.Mui-checked': {
                    color: '#bbdefb',
                  },
                  '& .MuiSvgIcon-root': {
                    fontSize: 20,
                  },
                }}
              />
              <ListItemText
                primary={option}
                primaryTypographyProps={{ fontSize: '0.9rem' }} // ✅ Smaller text
              />
            </MenuItem>
          ))}
        </Select>
      </FormControl>
    </Box>
  );
};

export default MergeRuleSelector;
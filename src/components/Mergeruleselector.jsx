import React from 'react';
import { FormControl, InputLabel, Select, MenuItem } from '@mui/material';

const MergeRuleSelector = ({ isCrossSystem, mergeRule, onChange }) => {
  const handleChange = (event) => {
    onChange(event.target.value);
  };

  return (
    <FormControl fullWidth style={{ marginTop: 24 }}>
      <InputLabel id="merge-rule-label">Merge Rule</InputLabel>
      <Select
        labelId="merge-rule-label"
        value={mergeRule}
        label="Merge Rule"
        onChange={handleChange}
      >
        {isCrossSystem ? (
          <MenuItem value="source-system-precedence">
            Source System Precedence (PS91 → PS92 → Medtox)
          </MenuItem>
        ) : (
          <MenuItem value="transaction-date">
            Transaction Date Precedence
          </MenuItem>
        )}
      </Select>
    </FormControl>
  );
};

export default MergeRuleSelector;

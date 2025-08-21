// Updated Sidebar.jsx
import React from 'react';
import Drawer from '@mui/material/Drawer';
import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';
import Divider from '@mui/material/Divider';
import Paper from '@mui/material/Paper';
import Chip from '@mui/material/Chip';

const drawerWidth = 240;

const Sidebar = ({ downloads }) => (
  <Drawer
    sx={{
      width: drawerWidth,
      flexShrink: 0,
      '& .MuiDrawer-paper': {
        width: drawerWidth,
        boxSizing: 'border-box',
        backgroundColor: '#222222',
        color: '#fff',
      },
    }}
    variant="permanent"
    anchor="left"
  >
    <Box sx={{ p: 2, display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
      <img src="/ernst-and-young.png" alt="EY Logo" style={{ width: 50, marginBottom: 10, filter: 'drop-shadow(0 0 4px white)'  }} 
/>

      <Typography variant="h6" sx={{ color: '#FFCD00' }}>Dedup Engine</Typography>
      <Typography variant="caption" sx={{ color: '#aaa', textAlign: 'center', mt: 1 }}>
        Harmonization Interface
      </Typography>
    </Box>

    <Divider sx={{ borderColor: '#444' }} />

    {/* Processing Modes Info */}
    <Box sx={{ p: 2 }}>
      <Typography variant="body2" sx={{ color: '#aaa', mb: 2 }}>Processing Modes</Typography>
      
      <Paper sx={{ p: 1.5, mb: 2, bgcolor: '#333', color: '#fff' }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
          <Chip label="Single File" size="small" color="warning" />
          <Typography variant="caption">Mode</Typography>
        </Box>
        <Typography variant="caption" sx={{ color: '#ccc' }}>
          Harmonize individual files and generate reusable outputs
        </Typography>
      </Paper>

      <Paper sx={{ p: 1.5, bgcolor: '#333', color: '#fff' }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
          <Chip label="Cross-System" size="small" color="primary" />
          <Typography variant="caption">Mode</Typography>
        </Box>
        <Typography variant="caption" sx={{ color: '#ccc' }}>
          Harmonization across multiple source systems
        </Typography>
      </Paper>
    </Box>

    <Divider sx={{ borderColor: '#444' }} />

    {/* Recent Downloads */}
    {downloads?.length > 0 && (
      <Box sx={{ px: 2, pb: 2 }}>
        <Typography variant="body2" sx={{ color: '#aaa', mt: 2, mb: 1 }}>
          Recent Outputs ({downloads.length})
        </Typography>
        <Box sx={{ maxHeight: 200, overflowY: 'auto' }}>
          {downloads.map((file, index) => (
            <Box key={index} sx={{ mb: 1 }}>
              <a
                href={`http://localhost:5001/api/download/${file}`}
                target="_blank"
                rel="noopener noreferrer"
                style={{ 
                  color: '#FFCD00', 
                  fontSize: '0.75rem',
                  textDecoration: 'none',
                  display: 'block',
                  padding: '4px 8px',
                  backgroundColor: '#333',
                  borderRadius: '4px',
                  wordBreak: 'break-all'
                }}
              >
                📄 {file}
              </a>
            </Box>
          ))}
        </Box>
      </Box>
    )}

    {/* Footer */}
    <Box sx={{ mt: 'auto', p: 2, borderTop: '1px solid #444' }}>
      <Typography variant="caption" sx={{ color: '#666', textAlign: 'center', display: 'block' }}>
        EY Deduplication Engine v2.0
      </Typography>
    </Box>
  </Drawer>
);

export default Sidebar;
// // src/components/FileList.jsx
// import React from 'react';
// import Typography from '@mui/material/Typography';
// import List from '@mui/material/List';
// import ListItemButton from '@mui/material/ListItemButton';
// import ListItemText from '@mui/material/ListItemText';

// const FileList = ({ files, onFileSelect }) => (
//   <div>
//     <Typography variant="h5" gutterBottom>Available Files</Typography>
//     <List>
//       {files.map(file => (
//         <ListItemButton
//           key={file}
//           onClick={() => {
//             console.log(`DEBUG: You clicked file: ${file}`);
//             onFileSelect(file);
//           }}
//         >
//           <ListItemText primary={file} />
//         </ListItemButton>
//       ))}
//     </List>
//   </div>
// );

// export default FileList;
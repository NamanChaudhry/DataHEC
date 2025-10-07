import React from 'react';

const ReportPage = () => {
  return (
    <div style={{ height: "95vh", width: "100%", background: "#f4f6fb" }}>
      <iframe
        src="http://localhost:5001/reports"
        title="Deduplication Reports Dashboard"
        width="100%"
        height="100%"
        style={{
          border: "none",
          boxShadow: "0 2px 15px rgba(0,0,0,0.12)"
        }}
      />
    </div>
  );
};

export default ReportPage;

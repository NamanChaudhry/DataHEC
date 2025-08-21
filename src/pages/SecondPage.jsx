import React from "react";
import "./SecondPage.css";

const SecondPage = ({ onConfigureClick }) => {
  console.log("Rendering SecondPage ✅");
  return (
    <div className="page-container">
      <div className="sidebar">
        <h2 className="logo">Menu</h2>
        <button>Entity</button>

        {/* IMPORTANT: Pass onConfigureClick to this button */}
        <button onClick={onConfigureClick}>Configure Harmonization</button>

        <button>Reports</button>
        <button>Match Rules</button>
        <button>Metadata</button>
        <button>Merge Rules</button>
        <button>Reconciliation</button>
        <button>Cross Walk</button>
      </div>

      <div className="content">
        <h1>DataHEC</h1>
        <h2>How to Use This Tool</h2>
        <ol>
          <li>
            <b>Single File Mode:</b> Process individual files and generate outputs
            that can be reused later
          </li>
          <li>
            <b>Cross-System Mode:</b> Select multiple files for global
            deduplication
          </li>
          <li>
            <b>Workflow:</b> Process files individually first, then use those
            outputs in cross-system mode for comprehensive deduplication
          </li>
        </ol>
      </div>
    </div>
  );
};

export default SecondPage;




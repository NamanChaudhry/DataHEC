import React, { useEffect, useState } from "react";
import axios from "axios";

export default function FileSelector({ entity, sourceSystem, setFileName }) {
  const [files, setFiles] = useState([]);

  useEffect(() => {
    axios
      .get(`http://localhost:5000/get_files/${entity}/${sourceSystem}`)
      .then((res) => setFiles(res.data));
  }, [entity, sourceSystem]);

  return (
    <div>
      <h2>Select File from {sourceSystem}</h2>
      {files.map((f) => (
        <button
          key={f}
          onClick={() => setFileName(f)}
          className="m-2 p-3 bg-purple-500 text-white rounded"
        >
          {f}
        </button>
      ))}
    </div>
  );
}

from flask import Flask, jsonify
from flask_cors import CORS
import os

app = Flask(__name__)
CORS(app)

@app.route('/')
def root():
    return jsonify({"message": "Flask is working!", "status": "ok"})

@app.route('/api/entities')
def get_entities():
    try:
        data_dir = 'data'
        if not os.path.exists(data_dir):
            return jsonify([])
        
        entities = [d for d in os.listdir(data_dir) if os.path.isdir(os.path.join(data_dir, d))]
        return jsonify(sorted(entities))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/health')
def health():
    return jsonify({"status": "healthy", "message": "Test app working"})

if __name__ == '__main__':
    print("🚀 Starting TEST Flask App on port 5000...")
    app.run(debug=True, host='127.0.0.1', port=5001)
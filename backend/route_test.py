from flask import Flask, jsonify

app = Flask(__name__)

@app.route("/api/match-rules", methods=["GET"])
def get_match_rules():
    return jsonify([
        {"rule": "Exact Match"},
        {"rule": "Fuzzy Match"},
        {"rule": "Email Match"}
    ])

if __name__ == "__main__":
    print("🔥 Starting test Flask server on http://localhost:5001")
    app.run(debug=True, port=5001)

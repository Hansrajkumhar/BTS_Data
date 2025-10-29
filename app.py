from flask import Flask, jsonify
from sheet import compute, google_cred, load_data

app = Flask(__name__)

@app.get("/healthz")
def health_check():
    return "✅ Server Live", 200

@app.get("/api/data")
def get_data():
    try:
        gc = google_cred()  # ✅ Create Google Sheets Client
        df, scddf, dffrs = load_data(gc)  # ✅ Pass it here

        result = compute(df, scddf, dffrs)

        # ✅ Convert DataFrame → JSON
        return jsonify(result.to_dict(orient="records"))

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)

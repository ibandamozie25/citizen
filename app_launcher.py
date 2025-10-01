
# app_launcher.py
from waitress import serve
from sec_app import app # import your Flask app object

if __name__ == "__main__":
    print("Starting School Manager… open http://127.0.0.1:1400 in your browser")
    serve(app, host="0.0.0.0", port=1400)

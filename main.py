"""
main.py — Application entry point

Run locally:  python main.py
Run via uvicorn: uvicorn main:app --reload
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from api.main import app

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
        reload=os.getenv("ENV", "production") == "development",
    )

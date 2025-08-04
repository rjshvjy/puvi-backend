"""
WSGI entry point for PUVI Oil Manufacturing System
Used by Gunicorn for production deployment
"""

from app import app

if __name__ == "__main__":
    app.run()

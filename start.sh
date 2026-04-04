#!/bin/bash
cd "$(dirname "$0")"
echo "Starting Problem Scouting UI..."
venv/bin/streamlit run ui/app.py

#!/usr/bin/env bash
echo "----------------------------------------"
date
echo "Starting script..."
source .venv/bin/activate
python main.py --log-level DEBUG
date
echo "Ending script..."
echo "----------------------------------------"

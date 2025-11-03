#!/usr/bin/env bash
# to be used with cron job
#00 */3 * * * /home/astek/PV-monitor/wrapper_main.sh >> /home/astek/PV-monitor/logs/cron.log 2>&1
echo "----------------------------------------"
date
echo "Starting main.py wrapper script..."
DIR="$(cd "$(dirname "$0")" && pwd)"
echo $DIR
cd $DIR
source .venv/bin/activate
python main.py --log-level INFO
PYTHONPATH=. python A5_analyze/analizy.py --log-level NOTICE
echo "Closing main.py wrapper script..."
date
echo "----------------------------------------"
echo "Starting postprocessing script..."
if [[ -x ./postprocessing.sh ]]; then
    ./postprocessing.sh
fi
echo "Closing postprocessing script..."
echo "----------------------------------------"

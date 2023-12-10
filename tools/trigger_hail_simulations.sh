#!/bin/zsh

#
# Sends an HTTP Post to the trigger_simulation_events function to kick off N hail simulations.
#

CLOUD_FUNCTION_URL='https://trigger-simulation-events-r3yb5kro3q-uc.a.run.app'
DEFAULT_NUM_SIMS=1000

example() {
  echo
  echo "___EXAMPLES___"
  echo
  echo "Kick off {DEFAULT_NUM_SIMS} in the cloud function"
  echo "> $this"
  echo
  echo "Run a smaller number of simulations"
  echo "> $this -n 10"
  echo
  echo "Test locally, sending to a function running in functions-framework"
  echo "> $this -url http://localhost:8080 -n 10"
  echo
}


usage() {
  echo "____USAGE_____"
  echo "$this [--url <url>] [--numsims <N>]"
  echo
  echo "Flags"
  echo "-u | --url - URL to send HTTP event."
  echo "-n | --numsims - Number of simulations to run."
  example
}

this=$0
url=$CLOUD_FUNCTION_URL
num_sims=$DEFAULT_NUM_SIMS

while [[ "$#" -gt 0 ]]; do
  case $1 in
    -u|--url) url="$2"; shift;;
    -n|--numsims) num_sims="$2"; shift;;
    -h|--help) usage; exit 0;;
  esac
  shift
done

cmd="""
curl -X POST $url -H 'Content-Type: application/json' \
-d '{\"storm_type\": \"hail\", \"num_sims\": $num_sims}'
"""

echo "Triggering simulations with:$cmd"

if read -q "choice?Press Y/y to continue: "; then
  echo
  echo "Here we go ..."
  eval $cmd
else
  echo
  echo "Exiting"
fi

#!/bin/zsh

# Script to help deploy the Google Cloud Function.
#
# Usage:
# tools/deploy_function.sh -h


DEFAULT_TOPIC_NAME="run-storm-simulation"


example() {
  echo
  echo "___EXAMPLES___"
  echo "Deploying the trigger function 'trigger_simulation_events'"
  echo "> $this trigger_simulation_events --project_id <project> --service_account simulator-cloud-functions"
  echo
  echo "Deploying the trigger function, with debugging, pointing to a test PubSub topic"
  echo "> $this trigger_simulation_events --project_id <project> --topic <topic> --envvars LOG_LEVEL=DEBUG"
  echo
  echo "Deploying the simulation function, 'run_storm_simulation'"
  echo "> $this run_storm_simulation --project_id <project> --service_account simulator-cloud-functions"
  echo
  echo "Deploying the simulation function, with debugging, writing a different BigQuery Dataset"
  echo "> $this run_storm_simulation --project_id <project> --envvars LOG_LEVEL=DEBUG,BIGQUERY_DATASET=<dataset>"
  echo
}

usage() {
  echo "____USAGE_____"
  echo "$this <function name> [--project_id <project>] [--service_account <account>] [--vars X=Y,...] [--topic <topic>]"
  echo
  echo "Flags"
  echo "-p | --project_id - GCP project to deploy into. If unset, also checks PROJECT_ID envvar"
  echo "-a | --service_account - GCP account to run function."
  echo "-e | --envvars - Env Vars to pass to the function"
  echo "-t | --topic - PubSub topic to publish to, if a pubsub triggered function."
  example
}

# Required 1st argument is the function to deploy.
this=$0
deploy_function=$1
if [[ -z $deploy_function ]]; then
  usage
  exit 1
fi

# Parse optional arguments
project_id=${PROJECT_ID}
pubsub_topic=$DEFAULT_TOPIC_NAME
service_account=""

while [[ "$#" -gt 0 ]]; do
  case $1 in
    -p|--project_id|--project) project_id="$2"; shift;;
    -a|--service_account) service_account="$2"; shift;;
    -e|--envvars) envvars="$2"; shift;;
    -t|--topic) pubsub_topic="$2"; shift;;
    -h|--help) usage; exit 0;;
  esac
  shift
done

# Project ID is required.
if [[ -z $project_id ]]; then
  echo "Need to set PROJECT_ID env var or --project_id flag"
  usage
  exit 1
fi

# Custom setup for each function.
case $deploy_function in
  "run_storm_simulation")
    trigger="--trigger-topic $pubsub_topic"
    memory="--memory=512MiB"
    timeout="540s"
  ;;
  "trigger_simulation_events")
    trigger="--trigger-http"
    memory="--memory=512MiB"
    timeout="3600s"
  ;;
  *) echo "Unsupported deployment function \"$deploy_function\""; exit 1;;
esac

if [[ ! -z $service_account ]]; then
  service_account="--service-account ${service_account}@${project_id}.iam.gserviceaccount.com"
fi

echo "*** Cloud Function Deployment ***"
echo "Func name: $deploy_function"
echo "Project id: $project_id"
echo "Service account $service_account"
echo "Trigger: $trigger"

echo "***"
echo

cmd="""
gcloud functions deploy $deploy_function \
 --project $project_id \
 $service_account \
 --allow-unauthenticated \
 --gen2 \
 --runtime=python311 \
 ${trigger} \
 ${memory} \
 --timeout $timeout \
 --set-env-vars PROJECT_ID=$project_id,$envvars
"""

echo "Deploying function with: $cmd"

if read -q "choice?Press Y/y to continue: "; then
  echo
  echo "Here we go ..."
  eval $cmd
else
  echo
  echo "Exiting"
fi

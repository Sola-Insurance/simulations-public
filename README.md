# Sola Insurance Storm Simulations

## Directory Structure
```
|-data/ - constant data of all flavors
|-db/ - definitions and utility classes for datastores, including Mongo and Bigquery
|-functions/ - (legacy directory) the simulation code
|-gcp/ - Google Cloud Platform helpers
|-geo/ - Geography logic
|-storm/ - Storm-specific logic
|-tools/ - Top-level scripts, deploying Google Cloud Functions.
|-main.py - Google Cloud Functions, see more below
```

## Testing

To run all tests in the project:

`python -m unittest`

To run a specific test in a submodule:

`python -m unitest discover <directory>`


## Running Simulations Locally

First run populate_headers.py

`python functions/populate_headers.py`


Then run generate_hailstorms.py

**Note: This will connect to MongoDB and write into simulation collections!**

`python functions/generate_hailstorms.py`


### Errors

#### SSL CERTIFICATE_VERIFY_FAILED
If you get an SSL cert verification failure when trying to connect to MongoDb, the likely cause (at least on new Mac OS)
is that the Python install is not using your system's SSL certificates. 

If you see an error like this:
**`[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed`**

then try this in your pip env:
```commandline
CERT_PATH=$(python -m certifi)                                                                                                                                                                                             1 ↵
export SSL_CERT_FILE=${CERT_PATH}
export REQUESTS_CA_BUNDLE=${CERT_PATH}
```
You only need this once per session.

Full stack of such an error:
```
Traceback (most recent call last):
    raise ServerSelectionTimeoutError(
pymongo.errors.ServerSelectionTimeoutError: dat-model-shard-00-02.2nng2.mongodb.net:27017: 
[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: unable to get local issuer certificate (_ssl.c:992),dat-model-shard-00-00.2nng2.mongodb.net:27017: [SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: unable to get local issuer certificate (_ssl.c:992),dat-model-shard-00-01.2nng2.mongodb.net:27017: [SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: unable to get local issuer certificate (_ssl.c:992), Timeout: 30s, Topology Description: <TopologyDescription id: 64232ff6aeae3090fa6a8799, topology_type: ReplicaSetNoPrimary, servers: [<ServerDescription ('dat-model-shard-00-00.2nng2.mongodb.net', 27017) server_type: Unknown, rtt: None, error=AutoReconnect('dat-model-shard-00-00.2nng2.mongodb.net:27017: [SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: unable to get local issuer certificate (_ssl.c:992)')>, <ServerDescription ('dat-model-shard-00-01.2nng2.mongodb.net', 27017) server_type: Unknown, rtt: None, error=AutoReconnect('dat-model-shard-00-01.2nng2.mongodb.net:27017: [SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: unable to get local issuer certificate (_ssl.c:992)')>, <ServerDescription ('dat-model-shard-00-02.2nng2.mongodb.net', 27017) server_type: Unknown, rtt: None, error=AutoReconnect('dat-model-shard-00-02.2nng2.mongodb.net:27017: [SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: unable to get local issuer certificate (_ssl.c:992)')>]>
```


## Google Cloud Functions

Google Cloud Functions deploy from `main.py`. Inside that file, there are  TWO deployed functions:
1) `trigger_simulation_events` - HTTP Triggered, for kicking off N parallel simulations
2) `run_storm_simulation` - CloudEvent/PubSub Triggered, for running a single simulation

The intended usage is to trigger the HTTP function (#1) with the configuration of which simulations to start. That 
function then sends the N PubSub events to trigger the CloudEvent function (#2) N times in parallel. From the 
CloudEvent function, it will run the given simulated storm.

The two functions are needed to achieve parallelism -- we want to fire-and-forget the trigger for sim #i. PubSub 
messaging allows for that. It's possible to also replace the HTTP function, using CloudEvents as the main trigger as 
well. There are two reasons to keep it as HTTP. First, legacy, it was written on HTTP to start. And, second, more 
importantly, PubSub guarantees __at least__ one execution. It's possible to redeliver a message. So, while not the 
end of the world, it'd be waste of resources to simultaneously run multiple N-storm-simulations.  

See [design doc](https://docs.google.com/document/d/1T1ILFRcgrEmGT6QCx6OmqEyT0efc7FC3tUWM_aPD0PU/edit#heading=h.84tnz0hcxacn)

```
 
Cloud Scheduler 
  --HTTP--> trigger_simulation_events 
              |--PubSub ->|
                          |--> run_storm_simulation(0)
                          |--> run_storm_simulation(1)
                          |...
                          |--> run_storm_simulation(N)
```

### Running Functions Locally

In one terminal, run:

#### trigger_simulation_events
```
SKIP_PUBSUB=1 functions-framework --target trigger_simulation_events
```

#### run_storm_simulation
```
functions-framework --target run_storm_simulation 
```

Additional, optional env-vars:
* `PROJECT_ID=<project>` - Set the GCP project ID.
* `LOG_LEVEL=DEBUG` - Set the logging level.  
* `PUBSUB_TOPIC=<topic>` - Override the default PubSub topic for testing
* `SKIP_PUBSUB=1` - Log everything, but don't send PubSub events. Very useful for testing flow. Remove to publish events
to a pubsub topic, but you should also set the PROJECT_ID and PUBSUB_TOPIC flags. 
* `BIGQUERY_OUTPUT_DATASET=<dataset>` - Override where output is written.

### Send Events to **Local** Functions

In a separate terminal...

#### trigger_simulation_events
```commandline
curl -X POST http://localhost:8080 \
  -H 'Content-Type: application/json' \
  -d '{"storm_type": "hail", "num_sims": 3}'
```

#### run_storm_simulation
```commandline
python tools/send_local_run_simulation_event.py
```

You should see logging in the first terminal window, handling the request. 


### Send Events to **Cloud** Functions

#### Trigger *all* simulations
_This sends an event to the trigger function, which creates the pubsub events_
You have two ways of doing this:
* Option 1: via Cloud Scheduler
  * goto [Cloud Scheduler Web Console](https://console.cloud.google.com/cloudscheduler)
  * find "run_hail_simulations", chose its three-dot menu at the right
  * click "Force Run"

* Option 2: via local command/curl
  * Run `tools/trigger_hail_simulations.sh [--num_sims N]`

#### Run select simulations
_This sends PubSub events to the run_storm_simulation function_

```commandline
# Command takes any of:
# - run a single simualtion with <id>
# - run a list of ids with "<id1>,<id2>,..."
# - run a range of ids with "<id1>-<id2>"
python tools/send_simulation_pubsub.py [sim ids]
```

### Deploying a Function

Use the local `deploy_function.sh` script to help with deployment.

First, set 

`export PROJECT_ID=favorable-mix-352819`


#### trigger_simulation_events
```
tools/deploy_function.sh trigger_simulation_events --project_id $PROJECT_ID --service_account simulator-cloud-functions
```

#### run_storm_simulation
```
tools/deploy_function.sh run_storm_simulation --project_id $PROJECT_ID --service_account simulator-cloud-functions
```

_Note, this may prompt for a region. You can choose different regions based on where you think you or
your users will be. [More info](https://cloud.google.com/functions/docs/locations#selecting_the_region)._


### Possible errors

If you get this error from _functions-framework_ when you send a curl, then try adding this env var:
```
OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
```
It seems like this occurs on some MacOS in certain scenarios. See
https://stackoverflow.com/questions/50168647/multiprocessing-causes-python-to-crash-and-gives-an-error-may-have-been-in-progr


### Future
* Turn on authentication requirements and see the following for both local and self-invocation:
https://cloud.google.com/functions/docs/securing/authenticating

* Consider deploying with some concurrency but it may be risky since there are some globals in the simulations:
https://cloud.google.com/functions/docs/configuring/concurrency

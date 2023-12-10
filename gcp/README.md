
# Google Cloud Platform (GCP)

This package provides library and tools to access the Google Cloud Platform (GCP).

Google Cloud's Python libraries are generally okay, though the style and quality of each service API may vary. This
can be annoying -- it's just how Google is. I've found it necessary to balance direct use of each client library with
wrapping some calls in my own utility libraries. For example, the Bigquery client has an API to insert rows which takes 
an iterator of the rows to insert. But, it rejects the write if the rows exceed a max size (10MB), so it's helpful to 
write a wrapper that provides a 'streaming' insert and automatically flushes if the number/size of rows gets too large.

The code in this package attempts to simplify/wrap some of the more common utility, but there's nothing wrong with 
directly using the clients it generates.


## Useful links

Setup:
* Download the [gcloud CLI pkg](https://cloud.google.com/sdk/docs/install) and place it somewhere in your PATH
* You'll get the python dependencies from `pip install -r requirements.txt`
* More background: https://cloud.google.com/apis/docs/client-libraries-explained


## Tests
`python -m unittest discover gcp/`


---


## Bigquery

Bigquery is GCP's main data-warehouse storage for table data. It is serverless, so you only pay for the data you store
and for data read by queries. It uses a custom SQL for queries so is easy to get started, but there are a few key 
differences. In Bigquery:
1. Tables are stored in `datasets`. These are containers that store tables, like directories to files in a filesystem. 
A dataset has policy like the datacenter where to store the tables, and table expiration. But generally, are a 
"create and forget" entity.
2. Every column is queryable without keys/indexes
2. UPDATE/DELETEs _are_ possible but generally discouraged. They actually cost more $ to run. Better to think of a
table as a static entity where data is written, and only making edits to correct errors. 
3. Bigquery 'partitioning' becomes very important as the scale of data grows. Partitioning is done by date, either the
date of row insertion or a custom column on the table. Conceptually, think of a table with partitions as a multi-
generational table, with each parition being a generation; or, think of a z-plane where time is the dimension with 
multiple instances of that table through time. The upside of this is we can keep multiple generations of table. The 
downside is that our queries need to be partition-aware to avoid aggregating across multiple generations or incurring
cost of querying a lot of data. 
4. Paritioning isn't required, but we need to think about it across multiple runs of the simulations. An alternative 
option to avoid all this is to delete and recreate an output table with each run.
5. We can import CSV files or directly stream data into a table. Streaming is better for running jobs, but there is 
a streaming-buffer that may take an undertermined amount of time to flush new rows.
6. Query execution is a shared resource, sometimes queries run slow if some other customer is using all the resources. 
One can pay for dedicated query resources. Not required, but something to think about.



Getting started:
https://cloud.google.com/bigquery/docs/quickstarts

Costs:
https://cloud.google.com/bigquery/pricing



#### Creating a dataset 

There is a [bigquery CLI](https://cloud.google.com/bigquery/docs/bq-command-line-tool) but the Web UI is often 
easier to manage the onetime setup of Datsets and tables.

Helpful guide: https://cloud.google.com/bigquery/docs/datasets#console

Quick and dirty, create a dataset:
1. Goto: https://console.cloud.google.com/bigquery
2. Look for the "Explorer" pane on the left hand-side. You should see your GCP project(s) listed, with a triangle to its
left and a three-dot menu to the right.
3. Click the three-dot menu, and choose "Create Dataset"
4. A right-hand sidebar will appear with the options: name, geo, table expiration. 
5. Once you click "Create Dataset" it'll appear in the Explorer on the right.


#### Create a table

First, make sure you have a dataset created (see above). 

Helpful guide: https://cloud.google.com/bigquery/docs/tables

Using the Web Console:
1. Goto: https://console.cloud.google.com/bigquery
2. Look for the "Explorer" pane on the left hand-side. You should see your GCP project(s) listed, with a triangle to its
left, click it to open the project and view datasets.
3. Find the dataset you want to add a table into, click its three-dot menu, and choose "Create table"
4. A right-hand sidebar will appear with the options -- creating an empty table vs importing data, tablename, and
partitioning. You can also specify its columns in the UI, pretty easy for small, one-off tables.
5. Once you click "Create Table" it'll appear in the Explorer on the right.


Using scripts:
* See the file `gcp/tools/create_table.py`. This has the python code for table creation and can run interactive to 
create a table and import a file into it.



### Code setup

To use _(this should already be in the requirements.txt)_:

`pip install --upgrade google-cloud-bigquery`

More info on Bigquery API libs:
https://cloud.google.com/bigquery/docs/reference/libraries

### Usage
See _bigquery/bigquery.py_ for creating a Bigquery API client and for inserting rows into a table. 


### Tools

See directory _bigquery/tools_
* create_table.py - An interactive script that can be used to create a Bigquery table with a given schema.

* write_table.py - An interactive script to import a data file into a Bigquery table. It also has a code example of how
  to stream rows into a table.

* query_table.py - An interactive script demonstrating how to query a table.


### Playing with some Sample Data

There's a very, very simple sample schema and CSV of data to get started with at `db/schema/sample`.
Once comfortable, we can delete those files and this section.

**Setup auth**

Either run `gcloud auth login` or get a keyfile and set _GOOGLE_APPLICATION_CREDENTIALS_ (see below)

**Create a dataset**
See [Creating a datasdet](#creating-a-dataset)

**Create a table** 

`python gcp/tools/create_table.py --project <PROJECT> <DATASET>.<TABLENAME> db/schema/sample/sample_schema.json`

**Populate the table**

`python gcp/tools/write_table.py <DATASET>.<TABLENAME> db/schema/sample/sample_data.csv --project  <PROJECT>`

** Query the table**

`python gcp/tools/query_table.py <PROJECT> --table <DATASET>.<TABLENAME>`

(or goto https://console.cloud.google.com/bigquery and click the table, then click "QUERY" at the top.)


---


## GCP Scope and Credentials

*TL;DR* Run `gcloud auth login` to authorize running as yourself or setenv `GOOGLE_APPLICATION_CREDENTIALS=<keyfile.json>`
to use a service account.

There are two systems at play which control who can access and what they can access in GCP.
* Credentials - provide the authorization to access GCP as a given user. In cloud parlance, this is 
  **Identity & Access Management** aka **IAM**. You can configure this on 
  Google's [IAM Cloud Console](https://console.cloud.google.com/iam-admin/iam) adding the users in your or and setting 
  roles for them, such as Viewer/Editor/Owner. For example, you can have a senior, trusted engineer as an Owner, a
  junior engineer as an Editor, and someone in Ops (who needs to query data) as a Viewer. 

* Scope - A given role/service may have different permissions as to what it can see and do on each GCP service. As a 
  practice this makes sense. But, TBH, from experience, can be a hassle to manage. 

When the code runs, in order to authorize access to GCP you must construct a client. And the
client will either implicitly generate credentials or can explicitly be passed existing credentials. 

If left implicitly the credentials are gotten from the environment. When running locally, you can use the `gcloud`
(see [here](https://cloud.google.com/docs/authentication/provide-credentials-adc))
command to login as a user which triggers an OAuth login flow (pops open a web page to login and authorize). This is 
okay for local dev but isn't tenable  for running automatically or once you have complex roles/scopes.

**Recommended**

The alternative is to use a [Service Account](https://cloud.google.com/iam/docs/service-account-overview), which is a
special account type for software to run authorized work. You can generate service accounts, assign each specific 
roles/scopes, and then manage key files (as .json files) with which the script can automatically authorize itself. One
passes the file (and a project id) as parameters to the code at the start.

To use a service_account's JSON key file, export this in your environment:
`GOOGLE_APPLICATION_CREDENTIALS=<path/to/key_file.json>` 

Or run a script like this:
```commandline
GOOGLE_APPLICATION_CREDENTIALS=<key_file.json> python ....
```

See also:
https://cloud.google.com/docs/authentication/client-libraries

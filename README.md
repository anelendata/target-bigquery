# target-bigquery

ANELEN's implementation of target-bigquery.

This is a [Singer](https://singer.io) target that loads JSON-formatted data
following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md)
to Google BigQuery.

## Installation

### Step 0: Acknowledge LICENSE and TERMS

Please especially note that the author(s) of target-bigquery is not responsible
for the cost, including but not limited to BigQuery cost) incurred by running
this program.

### Step 1: Activate the Google BigQuery API

(originally found in the [Google API docs](https://googlecloudplatform.github.io/google-cloud-python/latest/bigquery/usage.html))

 1. Use [this wizard](https://console.developers.google.com/start/api?id=bigquery-json.googleapis.com) to create or select a project in the Google Developers Console and activate the BigQuery API. Click Continue, then Go to credentials.
 2. On the **Add credentials to your project** page, click the **Cancel** button.
 3. At the top of the page, select the **OAuth consent screen** tab. Select an **Email address**, enter a **Product name** if not already set, and click the **Save** button.
 4. Select the **Credentials** tab, click the **Create credentials** button and select **OAuth client ID**.
 5. Select the application type **Other**, enter the name "Singer BigQuery Tap", and click the **Create** button.
 6. Click **OK** to dismiss the resulting dialog.
 7. Click the Download button to the right of the client ID.
 8. Move this file to your working directory and rename it *client_secrets.json*.


Export the location of the secret file:

```
export GOOGLE_APPLICATION_CREDENTIALS="./client_secret.json"
```

For other authentication method, please see Authentication section.

### Step 2: Install

First, make sure Python 3 is installed on your system or follow these 
installation instructions for Mac or Ubuntu.

```
pip install -U target-bigquery-partition
```

Or you can install the lastest development version from GitHub:

```
pip install --no-cache-dir https://github.com/anelendata/target-bigquery/archive/master.tar.gz#egg=target-bigquery
```

## Run

### Step 1: Configure

Create a file called target_config.json in your working directory, following 
config.sample.json:

```
{
    "project_id": "your-gcp-project-id",
    "dataset_id": "your-bigquery-dataset",
    "table_prefix": "optional_table_prefix",
    "table_ext": "optional_table_ext",
    "partition_by": "optional_column_name",
    "partition_type": "day",
    "partition_exp_ms": null,
    "stream": false,
}
```
Notes:
- The table name is set as stream name from the tap. You can add prefix and ext to the name.
- Optionally, you can set partition_by to create a partitioned table. Many production quailty taps implements a ingestion timestamp and it is recommended to use the column here to partition the table. It will increase the query performance and lower the BigQuery costs. partition_type can be hour, day, month, or year and the default is day. partition_exp_ms sets the partition expiration in millisecond. Default is null (never expire).
- `stream`: Make this true to run the streaming updates to BigQuery. Note that performance of batch update is better when keeping this option `false`.

### Step 2: Run

target-bigquery can be run with any Singer Target. As example, let use
[tap-exchangeratesapi](https://github.com/singer-io/tap-exchangeratesapi).

```
pip install tap-exchangeratesapi
```

Run:

```
tap-exchangeratesapi | target-bigquery -c target_config.json
```

## Authentication

It is recommended to use `target-bigquery` with a service account.

- Download the client_secrets.json file for your service account, and place it
  on the machine where `target-bigquery` will be executed.
- Set a `GOOGLE_APPLICATION_CREDENTIALS` environment variable on the machine,
  where the value is the fully qualified path to client_secrets.json

In the testing environment, you can also manually authenticate before runnig
the tap. In this case you do not need `GOOGLE_APPLICATION_CREDENTIALS` defined:

```
gcloud auth application-default login
```

You may also have to set the project:

```
gcloud config set project <project-id>
```

Though not tested, it should also be possible to use the OAuth flow to
authenticate to GCP as well:
- `target-bigquery` will attempt to open a new window or tab in your default
  browser. If this fails, copy the URL from the console and manually open it
  in your browser.
- If you are not already logged into your Google account, you will be prompted
  to log in.
- If you are logged into multiple Google accounts, you will be asked to select
  one account to use for the authorization.
- Click the **Accept** button to allow `target-bigquery` to access your Google BigQuery
  table.
- You can close the tab after the signup flow is complete.

## Original repo
https://github.com/anelendata/target-bigquery

# About this project

This project is developed by
ANELEN and friends. Please check out the ANELEN's
[open innovation philosophy and other projects](https://anelen.co/open-source.html)

![ANELEN](https://avatars.githubusercontent.com/u/13533307?s=400&u=a0d24a7330d55ce6db695c5572faf8f490c63898&v=4)
---

Copyright &copy; 2020~ Anelen Co., LLC

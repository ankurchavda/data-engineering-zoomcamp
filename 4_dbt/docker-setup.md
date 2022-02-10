# dbt with BigQuery on Docker

This is a quick guide on how to setup dbt with BigQuery on Docker.

Note: You will need your authentication json key file for this method to work. You can use oauth alternatively.

- Create a directory with the name of your choosing. I named it `4_dbt` indicating the 4th week in the course. 
  ```
  mkdir 4_dbt
  ```
- cd into the directory
  ```
  cd 4_dbt
  ```
- Copy this [Dockerfile](Dockerfile) in your directory. I borrowed it from the official dbt git [here](https://github.com/dbt-labs/dbt-core/blob/main/docker/Dockerfile)
- Create `docker-compose.yaml` file.
  ```yaml
  version: '3'
    services:
    dbt-de-zoomcamp:
        build:
            context: .
            target: dbt-bigquery
        volumes:
            - .:/usr/app
            - ~/.dbt/:/root/.dbt/
            - ~/.google/credentials/google_credentials.json:/.google/credentials/google_credentials.json
        network_mode: host
  ```
  -   Name the service as you deem right or `dbt-de-zoomcamp`.
  -   Use the `Dockerfile` in the current directory to build the image by passing `.` in the context.
  -   target specifies that we want to install the `dbt-bigquery` plugin in addition to `dbt-core`.
  -  Mount 3 volumes -
     - for persisting dbt data
     - path to the dbt `profiles.yml`
     - path to the `google_credentials.json`

- Create `profiles.yml` in `~/.dbt/` in your local machine
  ```yaml
  de-dbt-bq:
  outputs:
    dev:
      dataset: dbt_ankur
      fixed_retries: 1
      keyfile: /.google/credentials/google_credentials.json
      location: EU
      method: service-account
      priority: interactive
      project: dtc-ny-taxi
      threads: 4
      timeout_seconds: 300
      type: bigquery
  target: dev
  ```
  - Name the profile. `de-dbt-bq` in my case. This will be used in the `dbt_project.yml` file to refer and initiate dbt.
  - Replace with your `dataset`, `location` (my GCS bucket is in EU region), `project` values.
- Run the following commands -
  - ```bash 
    docker compose build 
    ```
  - ```bash 
    docker compose run dbt-de-zoomcamp init
    ``` 
    - Note: we are essentially running `dbt init` above because the `ENTRYPOINT` in the [Dockerfile](Dockerfile) is `['dbt']`.
    - Input the required values. Project name will be `taxi_rides_ny`
    - This should create `dbt/taxi_rides_ny/` and you should see `dbt_project.yml` in there.
    - In `dbt_project.yml`, replace `profile: 'taxi_rides_ny'` with `profile: 'de-dbt-bq'` as we have profile with the later name in the `profiles.yml`
  - ```bash
    docker compose run --workdir="/usr/app/dbt/taxi_rides_ny" dbt-de-zoomcamp debug
     ``` 
    - to test your connection. This shall output `All checks passed!` in the end.
    - Note: I had troubles running the above command in git bash. The --workdir flag took the local path instead of the path on local. Others have also faced this [issue](https://github.com/docker/cli/issues/2204#issuecomment-636453401).
    - Also, we change the working directory to the dbt project because the `dbt_project.yml` file should be in the current directory. Else it will throw `1 check failed: Could not load dbt_project.yml`
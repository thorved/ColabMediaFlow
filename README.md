# ColabMediaFlow

ColabMediaFlow is a local Go and Gin API that creates a Google Drive notebook and runs it on a hosted Google Colab CPU runtime. The worker scans Drive for ZIP and RAR archives, extracts them on Colab, and uploads the results to Drive.

## Configuration

If you already have a configured `.env`, keep it. Otherwise, copy `.env.example` to `.env` and fill in these settings.

| Setting | Value |
| --- | --- |
| `GOOGLE_OAUTH_CLIENT_ID` and `GOOGLE_OAUTH_CLIENT_SECRET` | OAuth client credentials from a project with Google Drive API and Colab API enabled |
| `GOOGLE_OAUTH_REFRESH_TOKEN` | Offline user refresh token granted both `https://www.googleapis.com/auth/drive` and `https://www.googleapis.com/auth/colaboratory` |
| `DRIVE_DOWNLOAD_FOLDER_ID` | Drive ID of `Media/download` or another folder to scan |
| `NOTEBOOK_PARENT_FOLDER_ID` | Drive ID of the folder where the managed notebook belongs |
| `API_KEY` | Random local bearer key for API requests |
| `RUN_INTERVAL` | Go duration such as `1m` or `30s`. Defaults to `1m`. Set it to `0` to prepare the notebook at startup and run it manually. |
| `LISTEN_ADDR` | API listen address. Defaults to `127.0.0.1:8080` for native runs and `0.0.0.0:8080` inside the container. |
| `STATE_DB` | Local SQLite path. Defaults to `colabmediaflow.db` in the working directory. |

To issue replacement credentials, authorize your OAuth client with both scopes and offline access, then exchange the authorization code for a refresh token. The OAuth Playground can perform this flow for a web client configured with its redirect URI and the **Use your own OAuth credentials** option. Store the replacement secret and refresh token in `.env` and restart the service. The service obtains and refreshes access tokens automatically. [Google OAuth setup](https://developers.google.com/identity/protocols/oauth2/web-server) · [OAuth Playground](https://developers.google.com/oauthplayground/)

## Run on Windows

Use Go 1.26 or newer. The service uses a pure Go SQLite driver and builds with CGO disabled.

```powershell
$env:CGO_ENABLED = '0'
go run ./cmd/colabmediaflow
```

## Run with Docker

The image builds with `CGO_ENABLED=0` and runs as a non-root user. Docker supplies `.env` values at startup and stores SQLite state in the named volume. The published API is accessible at `127.0.0.1:8080` on the host. [Docker port publishing](https://docs.docker.com/engine/network/port-publishing/)

```powershell
docker build --tag colabmediaflow:local .
docker run --rm --name colabmediaflow --env-file .env `
    --env LISTEN_ADDR=0.0.0.0:8080 --env STATE_DB=/data/colabmediaflow.db `
    --publish 127.0.0.1:8080:8080 `
    --mount type=volume,source=colabmediaflow-data,target=/data `
    colabmediaflow:local
```

Reuse `colabmediaflow-data` when replacing the container. A new volume starts with fresh state and the service sets up the notebook automatically. Run one service instance per workflow.

## Use the API

On startup the service creates or updates its managed notebook and starts a run immediately. It resumes an active run found in SQLite before resuming its interval. The API is available during notebook setup and reports setup errors while the service retries. Use `PUT /v1/notebook` to restore the bundled notebook code on demand.

Check the workflow from another PowerShell terminal in the same checkout.

```powershell
$base = 'http://127.0.0.1:8080'
$apiKey = (Get-Content .env | Where-Object { $_ -like 'API_KEY=*' } | Select-Object -First 1) -replace '^API_KEY=', ''
$headers = @{ Authorization = "Bearer $apiKey" }

Invoke-RestMethod -Uri "$base/v1/status" -Headers $headers
```

The response includes the notebook setup status and the active or latest run. `scanned` is the number of ZIP and RAR archives found in that run’s Drive scan. `processed`, `skipped`, `failed`, `remaining`, and `progress_percent` show how far that scan has progressed. Failed and skipped archives count as checked for the current run. `remaining` and `progress_percent` are `null` until scanning finishes. While an archive is active, `current_archive` shows its phase and, during a transfer or extraction, bytes done and total bytes for that operation. The same progress fields are available on `GET /v1/runs/{id}`.

Queue a manual run and query its status.

```powershell
$run = Invoke-RestMethod -Method Post -Uri "$base/v1/runs" -Headers $headers
Invoke-RestMethod -Method Get -Uri "$base/v1/runs/$($run.id)" -Headers $headers
```

Use an explicit retry to recheck previously rejected archives.

```powershell
Invoke-RestMethod -Method Post -Uri "$base/v1/runs" -Headers $headers `
    -ContentType 'application/json' -Body '{"retry_failed":true}'
```

The service logs scheduled run IDs. The status API reports `queued`, `running`, `cleaning_up`, `succeeded`, `partial`, or `failed`. A concurrent manual request returns HTTP 409 with the active run ID. The timer skips busy intervals and records empty scans as successful runs. Keep the service running for scheduled execution and keep its SQLite file for restart recovery.

### Use Bruno in VS Code

Open the repository root in VS Code. Install the recommended **Bruno** extension if prompted. The **ColabMediaFlow API** collection appears under **Explorer → Bruno Collections**. Open any request there or from the `bruno/` folder.

Select the **Local** environment and set its secret `API_KEY` to the value from your local `.env`. The default URL is `http://127.0.0.1:8080`. Send **Workflow status** first. It selects the active or latest run for **Run details**. Use the **Actions** folder to start a run, retry failed archives, or repair the notebook.

## Drive workflow

- The notebook scans standalone ZIP and RAR files directly inside the input folder. It preserves nested paths under `uncompressed_files/<archive stem>` and skips existing output files.
- After an archive’s outputs are present, the worker moves the original to `compressed_files` and appends a numeric suffix if the name is already used there.
- Archives with errors stay in the input folder. The scheduler retries transient failures and skips a corrupt, password-protected, or unsafe archive version until it changes or receives an explicit manual retry.
- Archives and extracted members travel directly between Drive and Colab. The runtime needs disk space for the compressed archive and its largest extracted member at the same time. The service allows up to six hours per run, subject to the Colab account’s runtime and usage limits. [Colab runtime limits](https://research.google.com/colaboratory/faq.html)

## Tests

```powershell
$env:CGO_ENABLED = '0'
go test ./...
python -m unittest discover -s internal/app/notebook -v
```

Install `rarfile==4.5` in your local Python environment to include the RAR fixture in the worker tests. The Colab worker sets up its own RAR support when needed.

The optional live test uses the OAuth settings from `.env` and provisions a temporary Drive folder and Colab CPU runtime for the test. It verifies automatic notebook setup and startup execution, the status API, extracted Drive file contents, archive movement, notebook reuse, and an empty scheduled run before cleaning up its test resources.

Set `COLAB_MEDIA_FLOW_LIVE_RAR_FIXTURE` to the path of a small, standalone RAR fixture to include RAR in the live test.

```powershell
$env:COLAB_MEDIA_FLOW_LIVE_TEST = '1'
go test ./internal/app -run '^TestLiveColabWorkflow$' -v -count=1 -timeout=10m
Remove-Item Env:\COLAB_MEDIA_FLOW_LIVE_TEST
```

## Project layout

| Directory | Responsibility |
| --- | --- |
| `cmd/colabmediaflow` | Executable and process signal handling |
| `internal/app` | Gin API, scheduler, run recovery, SQLite state, and managed notebook |
| `internal/app/notebook` | Embedded Python worker and archive tests |
| `internal/googleapi` | Google OAuth, Drive, and Colab clients |

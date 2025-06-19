# CUR Ingestion App Helm Chart

This Helm chart deploys the CUR (Cost and Usage Report) Ingestion Application to a Kubernetes cluster.

## Prerequisites

- Kubernetes 1.16+
- Helm 3.0+
- A Google Cloud Platform project with BigQuery and GCS enabled
- Appropriate IAM permissions for the service account

## Installation

### Add required dependencies to build.gradle

Ensure your project has the SnakeYAML dependency for YAML parsing:

```gradle
dependencies {
    // Existing dependencies
    implementation 'org.yaml:snakeyaml:1.29'
}
```

### Build the application

```bash
./gradlew build
```

### Build the Docker image

```bash
docker build -t gcr.io/your-project/cur-ingestion-app:latest .
```

### Push the Docker image to your registry

```bash
docker push gcr.io/your-project/cur-ingestion-app:latest
```

### Install the Helm chart

```bash
helm install cur-ingestion ./helm/cur-ingestion-app \
  --set config.gcp.projectId=your-project-id \
  --set config.gcs.bucket=your-gcs-bucket \
  --set config.cur.filePath=path/to/cur-file.csv \
  --set config.bigquery.table=dataset.table \
  --set image.repository=gcr.io/your-project/cur-ingestion-app
```

## Configuration

The following table lists the configurable parameters of the CUR Ingestion App chart and their default values.

| Parameter | Description | Default |
| --------- | ----------- | ------- |
| `replicaCount` | Number of replicas | `1` |
| `image.repository` | Image repository | `gcr.io/your-project/cur-ingestion-app` |
| `image.pullPolicy` | Image pull policy | `IfNotPresent` |
| `image.tag` | Image tag | `latest` |
| `config.gcp.projectId` | GCP project ID | `your-project-id` |
| `config.gcp.serviceAccountKey` | GCP service account key path | `""` |
| `config.gcs.bucket` | GCS bucket name | `your-gcs-bucket` |
| `config.cur.filePath` | Path to CUR file in GCS | `path/to/cur-file.csv` |
| `config.bigquery.table` | BigQuery table (dataset.table) | `dataset.table` |
| `config.bigquery.tempBucket` | Temporary GCS bucket for BigQuery operations | `temp-bucket-for-bigquery` |
| `config.tagging.rulesFile` | Path to tagging rules file | `/etc/cur-ingestion/tagging-rules.properties` |
| `config.tagging.useScd` | Whether to use SCD tagging | `false` |
| `config.tagging.mode` | Tagging mode (regular, scd, direct) | `regular` |
| `taggingRules` | List of tagging rules | See `values.yaml` |
| `resources` | CPU/Memory resource requests/limits | See `values.yaml` |

## Using a custom values file

Create a custom values file (e.g., `my-values.yaml`) to override the default values:

```yaml
config:
  gcp:
    projectId: "my-project-id"
  gcs:
    bucket: "my-gcs-bucket"
  cur:
    filePath: "path/to/my-cur-file.csv"
  bigquery:
    table: "my_dataset.my_table"

taggingRules:
  - name: "CustomRule"
    field: "line_item_product_code"
    operator: "=="
    value: "CustomService"
    tag: "CustomTag"
```

Install the chart with your custom values:

```bash
helm install cur-ingestion ./helm/cur-ingestion-app -f my-values.yaml
```

## Authentication

The application can authenticate with GCP in several ways:

1. Using a Kubernetes service account with Workload Identity (recommended for production)
2. Using a GCP service account key mounted as a secret
3. Using the default authentication mechanism

For Workload Identity, add the appropriate annotations to the service account:

```yaml
serviceAccount:
  annotations:
    iam.gke.io/gcp-service-account: your-gcp-sa@your-project.iam.gserviceaccount.com
```

#!/bin/bash
set -e

# Configuration
DOCKER_REGISTRY="your-registry"  # Replace with your Docker registry
IMAGE_NAME="cur-java-spark"
IMAGE_TAG="latest"
NAMESPACE="spark-jobs"

# Build the application
echo "Building the application..."
mvn clean package

# Build the Docker image
echo "Building Docker image..."
docker build -t ${DOCKER_REGISTRY}/${IMAGE_NAME}:${IMAGE_TAG} .

# Push the Docker image
echo "Pushing Docker image to registry..."
docker push ${DOCKER_REGISTRY}/${IMAGE_NAME}:${IMAGE_TAG}

# Create namespace and RBAC resources if they don't exist
echo "Setting up Kubernetes namespace and RBAC..."
kubectl apply -f k8s/namespace-and-rbac.yaml

# Create GCP service account secret
if [ ! -f "service-account-key.json" ]; then
  echo "Error: service-account-key.json file not found!"
  echo "Please create a GCP service account key and save it as service-account-key.json"
  exit 1
fi

# Check if secret exists, if not create it
SECRET_EXISTS=$(kubectl get secret -n ${NAMESPACE} gcp-service-account 2>/dev/null || echo "not-exists")
if [ "$SECRET_EXISTS" == "not-exists" ]; then
  echo "Creating GCP service account secret..."
  kubectl create secret generic gcp-service-account \
    --from-file=key.json=service-account-key.json \
    -n ${NAMESPACE}
else
  echo "GCP service account secret already exists."
fi

# Deploy the Spark application
echo "Deploying Spark application..."
kubectl apply -f k8s/spark-application.yaml

echo "Deployment complete! Monitor the application with:"
echo "kubectl get sparkapplications -n ${NAMESPACE}"
echo "kubectl describe sparkapplication cur-ingestion -n ${NAMESPACE}"
echo "kubectl logs -f -n ${NAMESPACE} cur-ingestion-driver"

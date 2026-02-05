#!/bin/bash
set -e

# Configuration
RESOURCE_GROUP="polybot-rg"
LOCATION="northeurope"
STORAGE_ACCOUNT="polybotfuncstorage"
FUNCTION_APP="polypoly-func"

echo "=== Polypoly Function App Deployment ==="
echo "Resource Group: $RESOURCE_GROUP"
echo "Location: $LOCATION"
echo "Function App: $FUNCTION_APP"
echo ""

# Check if logged in
if ! az account show &> /dev/null; then
    echo "Not logged in to Azure. Running 'az login'..."
    az login
fi

# Create resource group if it doesn't exist
echo "Creating resource group (if not exists)..."
az group create --name $RESOURCE_GROUP --location $LOCATION --output none 2>/dev/null || true

# Create storage account if it doesn't exist
echo "Creating storage account (if not exists)..."
az storage account create \
    --name $STORAGE_ACCOUNT \
    --resource-group $RESOURCE_GROUP \
    --location $LOCATION \
    --sku Standard_LRS \
    --output none 2>/dev/null || true

# Create function app if it doesn't exist
echo "Creating function app (if not exists)..."
az functionapp create \
    --resource-group $RESOURCE_GROUP \
    --consumption-plan-location $LOCATION \
    --runtime python \
    --runtime-version 3.11 \
    --functions-version 4 \
    --name $FUNCTION_APP \
    --storage-account $STORAGE_ACCOUNT \
    --os-type Linux \
    --output none 2>/dev/null || true

# Enable managed identity
echo "Enabling managed identity..."
PRINCIPAL_ID=$(az functionapp identity assign \
    --name $FUNCTION_APP \
    --resource-group $RESOURCE_GROUP \
    --query principalId -o tsv)
echo "Managed Identity Principal ID: $PRINCIPAL_ID"

# Configure app settings
echo "Configuring app settings..."
echo "NOTE: You need to set DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_CLUSTER_ID manually:"
echo "  az functionapp config appsettings set --name $FUNCTION_APP --resource-group $RESOURCE_GROUP \\"
echo "    --settings DATABRICKS_HOST=https://your-workspace.azuredatabricks.net \\"
echo "    DATABRICKS_TOKEN=dapi... \\"
echo "    DATABRICKS_CLUSTER_ID=xxxx-xxxxxx-xxxxxxxx \\"
echo "    ADLS_ACCOUNT_NAME=polybotdlso94zgo \\"
echo "    ADLS_ACCOUNT_NAME_BRONZE=polybotdlso94zgo"
echo ""

# Grant Storage Blob Data Contributor on the storage account
echo "Granting Storage Blob Data Contributor role on polybotdlso94zgo..."
az role assignment create \
    --assignee $PRINCIPAL_ID \
    --role "Storage Blob Data Contributor" \
    --scope "/subscriptions/$(az account show --query id -o tsv)/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.Storage/storageAccounts/polybotdlso94zgo" \
    --output none 2>/dev/null || echo "Role assignment may already exist"

# Deploy the function with remote build (required for Linux)
echo ""
echo "Deploying function app..."
func azure functionapp publish $FUNCTION_APP --build remote

echo ""
echo "=== Deployment Complete ==="
echo ""
echo "View logs for a specific function (e.g., events_refresh_timer):"
echo "  az monitor app-insights query --app $FUNCTION_APP --resource-group $RESOURCE_GROUP \\"
echo "    --analytics-query \"traces | where operation_Name == 'events_refresh_timer' | order by timestamp desc | take 50\""
echo ""
echo "Or in Azure Portal:"
echo "  Function App → $FUNCTION_APP → Functions → <function_name> → Monitor"

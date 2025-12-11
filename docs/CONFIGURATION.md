# Configuration & Placeholders

| Placeholder | Location | Description |
|-------------|----------|-------------|
| YOUR_SQL_SERVER | Notebooks, Linked Services | `adf-databricks.database.windows.net` |
| YOUR_SQL_USERNAME | Notebooks, LS | SQL Admin User |
| YOUR_SQL_PASSWORD | Notebooks, LS | SQL Admin Password |
| YOUR_STORAGE_ACCOUNT | Notebooks, LS | `adfdatabricks456` |
| YOUR_DATABRICKS_URL | Linked Service | `https://adb-....azuredatabricks.net` |
| YOUR_DATABRICKS_TOKEN | Linked Service | User Settings -> Developer -> Access Tokens |
| YOUR_CLIENT_ID | Mount Script | Service Principal ID |
| YOUR_SECRET | Mount Script | Service Principal Secret |

## Security Note
In a real production environment, never hardcode passwords. Use Azure Key Vault and reference secrets in ADF using `AzureKeyVaultLinkedService` and in Databricks using `dbutils.secrets.get()`.

# Setup/Mount_ADLS.py
dbutils.widgets.text("storage_account", "adfdatabricks456")
dbutils.widgets.text("container", "datalake")
dbutils.widgets.text("sp_client_id", "PLACEHOLDER_CLIENT_ID")
dbutils.widgets.text("sp_secret", "PLACEHOLDER_SECRET")
dbutils.widgets.text("sp_tenant_id", "PLACEHOLDER_TENANT_ID")

storage = dbutils.widgets.get("storage_account")
container = dbutils.widgets.get("container")
client_id = dbutils.widgets.get("sp_client_id")
secret = dbutils.widgets.get("sp_secret")
tenant = dbutils.widgets.get("sp_tenant_id")

configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": client_id,
    "fs.azure.account.oauth2.client.secret": secret,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant}/oauth2/token"
}

mount_point = "/mnt/datalake"
source = f"abfss://{container}@{storage}.dfs.core.windows.net/"

if not any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(source=source, mount_point=mount_point, extra_configs=configs)
    print(f"Mounted {mount_point}")
else:
    print(f"{mount_point} already mounted")

from google.oauth2 import service_account


class Credentials:
    """Utility class for building credentials for accessing Google Cloud services.

    Using this class isn't strictly necessary. Consider it an "advanced" mode if you have multiple credentials files
    for different service accounts and therefore can't use the GOOGLE_APPLICATION_CREDENTIALS flag. Instead, you can
    use this to explicitly create a Credentials from each file.

    Example usage:
    ```
        project_id = ...
        specific_credentials_file = ...
        credentials = Credentials.credentials(specific_credentials_file)
        bigquery_client = Bigquery.client(credentials=credentials, project_id=project_id)
    ```
    """

    @staticmethod
    def credentials(filepath: str) -> service_account.Credentials:
        """Generate credentials from the service account file at the given path."""
        return service_account.Credentials.from_service_account_file(filepath)

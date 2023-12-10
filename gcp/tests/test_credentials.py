# Copyright 2023 - Greg Hecht - All Rights Reserved
# Licensed to Sola Insurance, all modifications and reuse permitted within that organization.

import unittest

from gcp.credentials import Credentials
from unittest.mock import patch


class CredentialsTestCase(unittest.TestCase):
    @patch('gcp.credentials.service_account.Credentials.from_service_account_file')
    def test_credentials_passes_filepath(self, credentials_from_service_account_file):
        filepath = 'fake/file/path'
        credentials = Credentials.credentials(filepath)
        self.assertIs(credentials_from_service_account_file.return_value, credentials)
        credentials_from_service_account_file.assert_called_once_with(filepath)


if __name__ == '__main__':
    unittest.main()

"""Unit tests for Edwin credential environment variable aliases."""

import os
import unittest
from unittest.mock import patch

from elastic_poller import config


class EnvConfigAliasTests(unittest.TestCase):
    def test_prefers_edwin_names_over_legacy_aliases(self):
        with patch.dict(
            os.environ,
            {
                "EDWIN_ORG": "edwin-org",
                "DEXDA_ORG": "dexda-org",
                "EDWIN_ID": "edwin-id",
                "DEXDA_ID": "dexda-id",
                "EDWIN_TOKEN": "edwin-token",
                "DEXDA_TOKEN": "dexda-token",
            },
            clear=True,
        ):
            self.assertEqual(config.edwin_org(), "edwin-org")
            self.assertEqual(config.edwin_client_id(), "edwin-id")
            self.assertEqual(config.edwin_client_token(), "edwin-token")

    def test_falls_back_to_legacy_dexda_env_names(self):
        with patch.dict(
            os.environ,
            {
                "DEXDA_ORG": "dexda-org",
                "DEXDA_ID": "dexda-id",
                "DEXDA_TOKEN": "dexda-token",
            },
            clear=True,
        ):
            self.assertEqual(config.edwin_org(), "dexda-org")
            self.assertTrue(config.has_edwin_credentials())

    def test_missing_edwin_credentials(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertFalse(config.has_edwin_credentials())
            self.assertEqual(len(config.missing_edwin_credential_names()), 3)


if __name__ == "__main__":
    unittest.main()

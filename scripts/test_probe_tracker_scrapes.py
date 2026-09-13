"""Offline checks for scrape evidence classification; never contacts trackers."""
import unittest
from probe_tracker_scrapes import Incomplete, inspect_prefix, scrape_url


class ScrapeProbeTests(unittest.TestCase):
    def setUp(self):
        self.entry = (b'd5:filesd20:' + b'a' * 20 +
                      b'd8:completei1e10:downloadedi3e10:incompletei2eeee')

    def test_positive_bytes_and_mutable_network_buffer(self):
        for payload in (self.entry, bytearray(self.entry)):
            self.assertEqual(inspect_prefix(payload)[0], 'hashes_observed')

    def test_complete_first_entry_suffices_without_entire_catalog(self):
        self.assertEqual(inspect_prefix(self.entry[:-2])[0], 'hashes_observed')
        with self.assertRaises(Incomplete):
            inspect_prefix(self.entry[:35])

    def test_empty_is_not_positive(self):
        self.assertEqual(inspect_prefix(b'd5:filesdee')[0], 'empty_files')
        self.assertEqual(inspect_prefix(b'de')[0], 'no_files_dictionary')

    def test_explicit_rejection(self):
        self.assertEqual(inspect_prefix(b'd14:failure reason8:disablede'),
                         ('tracker_failure', 'disabled'))

    def test_http_success_or_invalid_stats_do_not_prove_support(self):
        for payload in (b'<html>OK</html>',
                        self.entry.replace(b'20:' + b'a' * 20, b'3:bad'),
                        self.entry.replace(b'i1e', b'i-1e'),
                        self.entry.replace(b'8:complete', b'8:whatever')):
            with self.assertRaises(ValueError):
                inspect_prefix(payload)

    def test_mapping_preserves_query_and_does_not_guess_udp(self):
        self.assertEqual(scrape_url('https://example.org/announce.php?key=x'),
                         'https://example.org/scrape.php?key=x')
        self.assertIsNone(scrape_url('udp://example.org:6969/announce'))
        self.assertIsNone(scrape_url('https://example.org/tracker'))


if __name__ == '__main__':
    unittest.main()

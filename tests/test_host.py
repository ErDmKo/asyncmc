from asyncmc.pool import Connection
from ._testutil import BaseTest, run_until_complete


class HostTests(BaseTest):
    def setUp(self):
        super(HostTests, self).setUp()

    def tearDown(self):
        super(HostTests, self).tearDown()

    @run_until_complete
    def test_hash_funtion(self):
        conn = Connection(servers=[
            'localhost:11211',
            'some_another_host:11211'
        ])
        s1, _ = conn._get_server('1')
        s2, _ = conn._get_server(b'1')
        self.assertEqual(s1, s2)

        s3, _ = conn._get_server('12')
        self.assertNotEqual(s1, s3)
        conn.close_socket()

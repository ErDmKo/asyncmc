from ._testutil import BaseTest
from asyncmc.host import Host


class HostTests(BaseTest):
    def setUp(self):
        super(HostTests, self).setUp()
        self.conn = Host(['127.0.0:1:11211'])

    def tearDown(self):
        super(HostTests, self).tearDown()

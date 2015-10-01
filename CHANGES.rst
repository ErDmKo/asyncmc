CHANGES
========

0.6.1 (1-10-2015)
---------------

- Bug fix and preparing to reconnect dead connections by errors

0.6 (2-07-2015)
---------------

- New feature Multiply server connection

0.5.1 (27-06-2015)
------------------

- Documentation improve

0.5 (25-06-2015)
----------------

- add support for noreply

the memcache protocol defines a 'noreply' optional parameter, which
instructs the server to not send a reply. In heavy usage environments
this can lead to significant performance improvements.

0.4 (19-06-2015)
----------------

- New features keys may be "str" typed and extension method "prepend" is "+" on python

0.3 (16-06-2015)
----------------

- New features new type of values "str" and extension method "append" is "+" on python

0.2 (15-06-2015)
----------------

- New features "add", "replace", "append", "prepend"

0.1 (10-06-2015)
----------------

- Initial release

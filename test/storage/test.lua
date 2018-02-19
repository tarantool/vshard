#!/usr/bin/env tarantool

require('strict').on()

box.cfg{
    listen              = os.getenv("LISTEN"),
}

require('console').listen(os.getenv('ADMIN'))

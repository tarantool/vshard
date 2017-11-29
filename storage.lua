#!/usr/bin/env tarantool

-- Get instance name
local fio = require('fio')
local NAME = fio.basename(arg[0], '.lua')

-- Check if we are running under test-run
if os.getenv('ADMIN') then
    test_run = require('test_run').new()
    require('console').listen(os.getenv('ADMIN'))
end

-- Call a configuration provider
cfg = require('devcfg')

-- Start the database with sharding
vshard = require('vshard')
vshard.storage.cfg(cfg, NAME)

box.once("testapp:schema:1", function()
    local customer = box.schema.space.create('customer')
    customer:format({
        {'customer_id', 'unsigned'},
        {'bucket_id', 'unsigned'},
        {'name', 'string'},
    })
    customer:create_index('customer_id', {parts = {'customer_id'}})
    customer:create_index('bucket_id', {parts = {'bucket_id'}, unique = false})

    local account = box.schema.space.create('account')
    account:format({
        {'account_id', 'unsigned'},
        {'customer_id', 'unsigned'},
        {'bucket_id', 'unsigned'},
        {'balance', 'unsigned'},
        {'name', 'string'},
    })
    account:create_index('account_id', {parts = {'account_id'}})
    account:create_index('customer_id', {parts = {'customer_id'}, unique = false})
    account:create_index('bucket_id', {parts = {'bucket_id'}, unique = false})
    box.snapshot()

    box.schema.func.create('customer_lookup')
    box.schema.role.grant('public', 'execute', 'function', 'customer_lookup')
    box.schema.func.create('customer_add')
    box.schema.role.grant('public', 'execute', 'function', 'customer_add')
end)

function customer_add(customer)
    box.begin()
    box.space.customer:insert({customer.customer_id, customer.bucket_id,
                               customer.name})
    for _, account in ipairs(customer.accounts) do
        box.space.account:insert({
            account.account_id,
            customer.customer_id,
            customer.bucket_id,
            0,
            account.name
        })
    end
    box.commit()
    return true
end

function customer_lookup(customer_id)
    if type(customer_id) ~= 'number' then
        error('Usage: customer_lookup(customer_id)')
    end

    local customer = box.space.customer:get(customer_id)
    if customer == nil then
        return nil
    end
    customer = {
        customer_id = customer.customer_id;
        name = customer.name;
    }
    local accounts = {}
    for _, account in box.space.account.index.customer_id:pairs(customer_id) do
        table.insert(accounts, {
            account_id = account.account_id;
            name = account.name;
            balance = account.balance;
        })
    end
    customer.accounts = accounts;
    return customer
end

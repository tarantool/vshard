-- vshard.util
local log = require('log')
local fiber = require('fiber')
local json = require('json')

--
-- Extract parts of a tuple.
-- @param tuple Tuple to extract a key from.
-- @param parts Array of index parts. Each part must contain
--        'fieldno' attribute.
--
-- @retval Extracted key.
--
local function tuple_extract_key(tuple, parts)
    local key = {}
    for _, part in ipairs(parts) do
        table.insert(key, tuple[part.fieldno])
    end
    return key
end

--
-- Wrapper to run @a func in infinite loop and restart it on the
-- module reload. This function CAN NOT BE AUTORELOADED. To update
-- it you must manualy stop all fibers, run by this function, do
-- reload, and then restart all stopped fibers. This can be done,
-- for example, by calling vshard.storage/router.cfg() again with
-- the same config as earlier.
--
-- @param func Reloadable function to run. It must accept current
--        module version as an argument, and interrupt itself,
--        when it is changed.
-- @param worker_name Name of the function. Usual infinite fiber
--        represents a background subsystem, which has a name. For
--        example: "Garbage Collector", "Recovery", "Discovery",
--        "Rebalancer".
-- @param M Module which can reload.
--
local function reloadable_fiber_f(M, func_name, worker_name)
    while true do
        local ok, err = pcall(M[func_name], M.module_version)
        if not ok then
            log.error('%s has been failed: %s', worker_name, err)
            fiber.yield()
        else
            log.info('%s has been reloaded', worker_name)
            fiber.yield()
        end
    end
end

--
-- Wrap a given function to check that self argument is passed.
-- New function returns an error in case one called a method
-- like object.func instead of object:func.
-- Returning wrapped function to a user and using raw functions
-- inside of a module improves speed.
-- This function can be used only in case the second argument is
-- not of the "table" type or has different metatable.
-- @param obj_name Name of the called object. Used only for error
--        message construction.
-- @param func_name Name of the called function. Used only for an
--        error message construction.
-- @param mt Meta table of self argument.
-- @param func A function which is about to be wrapped.
--
-- @retval Wrapped function.
--
local function generate_self_checker(obj_name, func_name, mt, func)
    return function (self, ...)
        if getmetatable(self) ~= mt then
            local fmt = 'Use %s:%s(...) instead of %s.%s(...)'
            error(string.format(fmt, obj_name, func_name, obj_name, func_name))
        end
        return func(self, ...)
    end
end

--
-- Get datastructure diff
-- old and new should not contain @add ...
-- @param old 
--
local service_fields = {'@add', '@del', '@change'}
local function data_diff(old, new, result)
    assert(type(new) == 'table') 
    assert(type(old) == 'table') 
    result = result or {}
    for _, sf in pairs(service_fields) do
        result[sf] = {}
        assert(old[sf] == nil)
        assert(new[sf] == nil)
    end
    for k, v in pairs(new) do
        if old[k] == nil then
            result['@add'][k] = table.deepcopy(v)
        end
        if type(v) == 'table' and type(old[k]) == 'table' then
            local child = {}
            result[k] = child
            data_diff(old, new, child)
            if not next(child) then
                resuld[k] = nil
            end
        elseif v ~= old[k] then
            result['@change'][k] = {old[k], v}
        end
    end
    for k, v in pairs(old) do
        if new[k] == nil then
            result['@del'][k] = table.deepcopy(v)
        end
    end
    for _, sf in pairs(service_fields) do
        if not next(result[sf]) then
            result[sf] = nil
        end
    end
    return result
end


local consistent_json
local function is_primitive_type(xtype)
    local ptypes = {"string", "number", "boolean"}
    for _,t in ipairs(ptypes) do
        if xtype == t then return true end
    end
    return false
end

local function consistent_json_array(data)
    local res = {}
    for _,item in ipairs(data) do
        table.insert(res,consistent_json(item))
    end
    return string.format("[%s]", table.concat(res, ","))
end

local function consistent_json_object(data)
    local res = {}
    local ordered_fnames = {}
    for k, v in pairs(data) do
        table.insert(ordered_fnames, k)
    end
    table.sort(ordered_fnames)
    for _, name in ipairs(ordered_fnames) do
        local item = data[name]
        local inner = consistent_json(item)
        inner = string.format([[%s:%s]], json.encode(name), inner)
        table.insert(res, inner)
    end
    return string.format("{%s}", table.concat(res, ","))
end

-- Takes lua data structure and produces consistent json.
-- Consistent means that the very same json would be created
-- for the same data.
-- @param data Lua data structure.
--
-- @retval consistend_json String. Json replesentation of the
--         data.
consistent_json = function (data)
    local xtype = type(data)
    if is_primitive_type(xtype) then
        return json.encode(data)
    end
    if xtype ~= "table" then
        raise_error("data type is not supported: %s", xtype)
    end
    -- array
    if #data > 0 then
        return consistent_json_array(data)
    end
    -- object (dict)
    return consistent_json_object(data)
end

return {
    tuple_extract_key = tuple_extract_key,
    reloadable_fiber_f = reloadable_fiber_f,
    generate_self_checker = generate_self_checker,
    consistent_json = consistent_json,
}

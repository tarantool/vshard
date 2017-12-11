function check_error(func, ...)
	local status, err = pcall(func, ...)
	assert(not status)
	err = string.gsub(err, '.*/[a-z]+.lua.*[0-9]+: ', '')
	return err
end

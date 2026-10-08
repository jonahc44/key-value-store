wrk.method = "PUT"
wrk.headers["Content-Type"] = "application/json"
local counter = 0

request = function()
  counter = counter + 1
  return wrk.format(nil, "/key" .. (counter % 1000), nil, '{"n":' .. counter .. '}')
end

done = function(summary, latency, requests)
  local errors = summary.errors
  io.write(string.format(
    'RESULT {"rps":%.1f,"p50_us":%d,"p99_us":%d,"max_us":%d,"requests":%d,"non2xx":%d,"socket_errors":%d}\n',
    summary.requests / (summary.duration / 1e6),
    latency:percentile(50), latency:percentile(99), latency.max,
    summary.requests, errors.status,
    errors.connect + errors.read + errors.write + errors.timeout))
end

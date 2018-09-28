# file_to_nsq config example

## config.json
`
{
    "ConsulAddress":"127.0.0.1:8500",
    "Datacenter":"spacex",
    "Token":"xxxx,
    "Cluster":"filetomq/clusters/rocket1",
    "StatsdAddr":"127.0.0.1:8125"
}
`
## consul key

key 'filetonsq/clusters/rocket1/xxxx'
value '{"A":{"Version":"aaa","SinkType":"kafka","SinkAddress":"127.0.0.1:12111,127.0.0.1:12211","Topic":"weblog","FileSettings":{"Name":"xx","BatchSize":10,"StartAtEOF":false}}}'

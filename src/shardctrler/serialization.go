package shardctrler

import (
	"encoding/json"
	"fmt"
)

/*
Since slice/map are not comparable, change it to be string
*/

func serialize(data interface{}) string {
	b, err := json.Marshal(data)
	if err != nil {
		panic(fmt.Sprintf("serializeServersJoined err, obj=%v", data))
	}
	return string(b)
}

// Deserialize map[int][]string
func deserializeServersJoined(data string) map[int][]string {
	var servers map[int][]string
	err := json.Unmarshal([]byte(data), &servers)
	if err != nil {
		panic(fmt.Sprintf("deserializeServersJoined err, obj=%v", data))
	}
	return servers
}

// Deserialize []int
func deserializeGidLeaved(data string) []int {
	var gids []int
	err := json.Unmarshal([]byte(data), &gids)
	if err != nil {
		panic(fmt.Sprintf("deserializeGidLeaved err, obj=%v", data))
	}
	return gids
}

// Deserialize Config
func deserializeQueryRes(data string) Config {
	var cfg Config
	if data == "" {
		return cfg
	}
	err := json.Unmarshal([]byte(data), &cfg)
	if err != nil {
		panic(fmt.Sprintf("deserializeQueryRes err, obj=%v", data))
	}
	return cfg
}

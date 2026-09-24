package pipeline

import "sort"

func sortedKeys[V any](m map[DeviceID]V) []DeviceID {
	keys := make([]DeviceID, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

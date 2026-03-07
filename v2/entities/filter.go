package entities

type Filters struct {
	Include map[string][]string
	Exclude map[string][]string
	Should  []ShouldFilter
	Ranges  map[string][]Range
}

type Range struct {
	Operation string
	Value     string
}

type ShouldFilter struct {
	Terms  map[string][]string
	Ranges map[string][]Range
}

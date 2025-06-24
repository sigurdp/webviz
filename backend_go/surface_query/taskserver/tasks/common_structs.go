package tasks

type RealizationSurfaceObject struct {
	Realization int    `json:"realization" binding:"required"`
	ObjectUuid  string `json:"objectUuid" binding:"required"`
}

type RealizationValues struct {
	Realization   int       `msgpack:"realization"  json:"realization"`
	SampledValues []float32 `msgpack:"sampledValues"       json:"sampledValues"`
}

type SampleInPointsTaskResult struct {
	RealizationSamples []RealizationValues `msgpack:"realizationSamples"  json:"realizationSamples"`
	UndefLimit         float32             `msgpack:"undefLimit"          json:"undefLimit"`
}

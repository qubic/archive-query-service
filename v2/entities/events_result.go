package entities

import api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"

type EventsResult struct {
	Hits   *Hits
	Events []*api.Event
}

func (r *EventsResult) GetHits() *Hits {
	if r == nil || r.Hits == nil {
		return &Hits{}
	}
	return r.Hits
}

func (r *EventsResult) GetEvents() []*api.Event {
	if r == nil || r.Events == nil {
		return make([]*api.Event, 0)
	}
	return r.Events
}

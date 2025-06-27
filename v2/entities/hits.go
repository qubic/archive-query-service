package entities

type Hits struct {
	Total    int
	Relation string
}

func (h *Hits) GetTotal() int {
	if h != nil {
		return h.Total
	} else {
		return 0
	}
}

func (h *Hits) GetRelation() string {
	if h != nil {
		return h.Relation
	} else {
		return ""
	}
}

package models

// QueryCoordinatesMessage message struct
type QueryCoordinatesMessage struct {
	Reference   string     `json:"reference"`
	Coordinates [][]string `json:"coordinates"`
}

// StoreDataMessage message struct
type StoreDataMessage struct {
	Reference string          `json:"reference"`
	Result    UKAPIPOSTResult `json:"result"`
}

// UKAPIBulkQuery struct to generate body body
type UKAPIBulkQuery struct {
	Geolocations []UKAPICoordinate `json:"geolocations"`
}

// UKAPICoordinate API coordintate struct
type UKAPICoordinate struct {
	Postcode  string  `json:"postcode"`
	Longitude float64 `json:"longitude"`
	Latitude  float64 `json:"latitude"`
	Radius    int     `json:"radius"`
	Limit     int     `json:"limit"`
}

// UKAPIPOSTResult post query result
type UKAPIPOSTResult struct {
	Status int            `json:"status"`
	Result []UKAPIResults `json:"result"`
}

// UKAPIResults API single solution struct
type UKAPIResults struct {
	Result []UKAPICoordinate `json:"result"`
}

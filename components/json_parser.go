package components

import "encoding/json"

type JsonParser struct {
}

func (*JsonParser) GmqParseData(data any) (dt any, err error) {
	bt, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(bt, &dt)
	return dt, err
}

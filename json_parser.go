package gmq

import "encoding/json"

type JsonParser struct {
	DateFormat string
}

func (*JsonParser) GmqParseData(data any) (dt any, err error) {
	bt, err := json.Marshal(data)
	if bt != nil {
		err = json.Unmarshal(bt, &dt)
	}
	return
}

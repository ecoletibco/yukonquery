package yukonquery

import "github.com/project-flogo/core/data/coerce"

type Settings struct {
	URL                string            `md:"url, required"`
	UcsConnectionId    string            `md:"ucsConnectionId"`
	UcsConnectionToken string            `md:"ucsConnectionToken"`
	ConnectorName      string            `md:"connectorName"`
	ConnectorProps     map[string]string `md:"connectorProps"`
	Query              string            `md:"query,required"`
}

type Input struct {
	Params map[string]interface{} `md:"params"`
}

type Output struct {
	EOF     bool                     `md:"eof"`
	Results []map[string]interface{} `md:"results"`
}

// FromMap converts the values from a map into the struct Input
func (i *Input) FromMap(values map[string]interface{}) error {
	params, err := coerce.ToObject(values["params"])
	if err != nil {
		return err
	}
	i.Params = params
	return nil
}

// ToMap converts the struct Input into a map
func (i *Input) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"params": i.Params,
	}
}

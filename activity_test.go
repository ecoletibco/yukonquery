package yukonquery

import (
	"testing"

	"github.com/project-flogo/core/activity"
	"github.com/project-flogo/core/data/mapper"
	"github.com/project-flogo/core/data/resolve"
	"github.com/project-flogo/core/support/test"
	"github.com/stretchr/testify/assert"
)

const (
	TestUrl                = "https://localhost:44346/api"
	TestUcsConnectionId    = ""
	TestUcsConnectionToken = ""
	TestConnectorName      = "Benchmark"
	TestQuery              = "select * from entity2"
)

var TestConnectorProps = map[string]string{
	"Username": "ecole@tibco.com",
	"Password": "XXXXX",
}

func TestRegister(t *testing.T) {

	ref := activity.GetRef(&Activity{})
	act := activity.Get(ref)

	assert.NotNil(t, act)
}

func TestSettings(t *testing.T) {

	// valid settings
	settings := &Settings{
		URL:                TestUrl,
		UcsConnectionId:    TestUcsConnectionId,
		UcsConnectionToken: TestUcsConnectionToken,
		ConnectorName:      TestConnectorName,
		ConnectorProps:     TestConnectorProps,
		Query:              TestQuery,
	}

	iCtx := test.NewActivityInitContext(settings, nil)
	_, err := New(iCtx)
	assert.Nil(t, err)

	// No URL
	settings = &Settings{
		URL:                "",
		UcsConnectionId:    TestUcsConnectionId,
		UcsConnectionToken: TestUcsConnectionToken,
		ConnectorName:      TestConnectorName,
		ConnectorProps:     TestConnectorProps,
		Query:              TestQuery,
	}

	iCtx = test.NewActivityInitContext(settings, nil)
	_, err = New(iCtx)
	assert.NotNil(t, err)

	// Bad URL
	settings = &Settings{
		URL:                "https://tibco.com",
		UcsConnectionId:    TestUcsConnectionId,
		UcsConnectionToken: TestUcsConnectionToken,
		ConnectorName:      TestConnectorName,
		ConnectorProps:     TestConnectorProps,
		Query:              TestQuery,
	}

	iCtx = test.NewActivityInitContext(settings, nil)
	_, err = New(iCtx)
	assert.NotNil(t, err)

	// No Query
	settings = &Settings{
		URL:                TestUrl,
		UcsConnectionId:    TestUcsConnectionId,
		UcsConnectionToken: TestUcsConnectionToken,
		ConnectorName:      TestConnectorName,
		ConnectorProps:     TestConnectorProps,
		Query:              "",
	}

	iCtx = test.NewActivityInitContext(settings, nil)
	_, err = New(iCtx)
	assert.NotNil(t, err)

	// No user
	tmpConnectorProps := TestConnectorProps
	tmpConnectorProps["Username"] = ""
	settings = &Settings{
		URL:                TestUrl,
		UcsConnectionId:    TestUcsConnectionId,
		UcsConnectionToken: TestUcsConnectionToken,
		ConnectorName:      TestConnectorName,
		ConnectorProps:     tmpConnectorProps,
		Query:              TestQuery,
	}

	iCtx = test.NewActivityInitContext(settings, nil)
	_, err = New(iCtx)
	assert.NotNil(t, err)

	// No password
	tmpConnectorProps = TestConnectorProps
	tmpConnectorProps["Password"] = ""
	settings = &Settings{
		URL:                TestUrl,
		UcsConnectionId:    TestUcsConnectionId,
		UcsConnectionToken: TestUcsConnectionToken,
		ConnectorName:      TestConnectorName,
		ConnectorProps:     tmpConnectorProps,
		Query:              TestQuery,
	}

	iCtx = test.NewActivityInitContext(settings, nil)
	_, err = New(iCtx)
	assert.NotNil(t, err)

	/*

		Benchmark does not suppoty this test

		// Bad creds
		tmpConnectorProps = TestConnectorProps
		tmpConnectorProps["Username"] = "BadUsername"
		tmpConnectorProps["Password"] = "BadPassword"
		settings = &Settings{
			URL:                TestUrl,
			UcsConnectionId:    TestUcsConnectionId,
			UcsConnectionToken: TestUcsConnectionToken,
			ConnectorName:      TestConnectorName,
			ConnectorProps:     TestConnectorProps,
			Query:              TestQuery,
		}

		iCtx = test.NewActivityInitContext(settings, nil)
		_, err = New(iCtx)
		assert.NotNil(t, err)
	*/
}

func TestEvalSimpleSelect(t *testing.T) {

	settings := &Settings{
		URL:                TestUrl,
		UcsConnectionId:    TestUcsConnectionId,
		UcsConnectionToken: TestUcsConnectionToken,
		ConnectorName:      TestConnectorName,
		ConnectorProps:     TestConnectorProps,
		Query:              TestQuery,
	}

	mf := mapper.NewFactory(resolve.GetBasicResolver())
	iCtx := test.NewActivityInitContext(settings, mf)
	act, err := New(iCtx)
	assert.Nil(t, err)

	tc := test.NewActivityContext(act.Metadata())

	//eval
	done, err := act.Eval(tc)
	assert.True(t, done)
	assert.Nil(t, err)

	assert.NotNil(t, tc.GetOutput("eof"))
	assert.NotNil(t, tc.GetOutput("results"))

	eof := tc.GetOutput("eof").(bool)
	assert.True(t, eof == false)

	results := tc.GetOutput("results").([]interface{})
	assert.True(t, len(results) == 250)
}

func TestEvalBadQuery(t *testing.T) {

	settings := &Settings{
		URL:                TestUrl,
		UcsConnectionId:    TestUcsConnectionId,
		UcsConnectionToken: TestUcsConnectionToken,
		ConnectorName:      TestConnectorName,
		ConnectorProps:     TestConnectorProps,
		Query:              "select * from BadTableName",
	}

	mf := mapper.NewFactory(resolve.GetBasicResolver())
	iCtx := test.NewActivityInitContext(settings, mf)
	act, err := New(iCtx)
	assert.Nil(t, err)

	tc := test.NewActivityContext(act.Metadata())

	//eval
	done, err := act.Eval(tc)
	assert.False(t, done)
	assert.NotNil(t, err)
}

func TestParseQuery(t *testing.T) {

	// basic select * query
	_, err := parseQuery("select * from entity2")
	assert.Nil(t, err)

	// basic select column query
	_, err = parseQuery("select index from entity2")
	assert.Nil(t, err)

	// basic select columns query
	_, err = parseQuery("select index, prop1 from entity2")
	assert.Nil(t, err)

	// blank query
	_, err = parseQuery("")
	assert.NotNil(t, err)

	// only select
	_, err = parseQuery("select")
	assert.NotNil(t, err)

	// no columns
	_, err = parseQuery("select from entity2")
	assert.NotNil(t, err)

	// no from
	_, err = parseQuery("select *")
	assert.NotNil(t, err)

	// no table
	_, err = parseQuery("select * from")
	assert.NotNil(t, err)

}

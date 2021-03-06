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

func TestEvalSimpleSelectAll(t *testing.T) {

	settings := &Settings{
		URL:                TestUrl,
		UcsConnectionId:    TestUcsConnectionId,
		UcsConnectionToken: TestUcsConnectionToken,
		ConnectorName:      TestConnectorName,
		ConnectorProps:     TestConnectorProps,
		Query:              "select * from entity2",
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

func TestEvalSimpleSelectTop10(t *testing.T) {

	settings := &Settings{
		URL:                TestUrl,
		UcsConnectionId:    TestUcsConnectionId,
		UcsConnectionToken: TestUcsConnectionToken,
		ConnectorName:      TestConnectorName,
		ConnectorProps:     TestConnectorProps,
		Query:              "select top 10 * from entity2",
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
	assert.True(t, len(results) == 10)
}

func TestEvalSimpleSelectSkip10(t *testing.T) {

	settings := &Settings{
		URL:                TestUrl,
		UcsConnectionId:    TestUcsConnectionId,
		UcsConnectionToken: TestUcsConnectionToken,
		ConnectorName:      TestConnectorName,
		ConnectorProps:     TestConnectorProps,
		Query:              "select skip 10 * from entity2",
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

	firstResult := results[0].(map[string]interface{})
	firstIndex := firstResult["Index"].(float64)
	assert.True(t, firstIndex == 11)
}

func TestEvalSimpleSelectTop10Skip10(t *testing.T) {

	settings := &Settings{
		URL:                TestUrl,
		UcsConnectionId:    TestUcsConnectionId,
		UcsConnectionToken: TestUcsConnectionToken,
		ConnectorName:      TestConnectorName,
		ConnectorProps:     TestConnectorProps,
		Query:              "select top 10 skip 10 * from entity2",
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
	assert.True(t, len(results) == 10)

	firstResult := results[0].(map[string]interface{})
	firstIndex := firstResult["Index"].(float64)
	assert.True(t, firstIndex == 11)
}

func TestEvalSimpleSelect2Columns(t *testing.T) {

	settings := &Settings{
		URL:                TestUrl,
		UcsConnectionId:    TestUcsConnectionId,
		UcsConnectionToken: TestUcsConnectionToken,
		ConnectorName:      TestConnectorName,
		ConnectorProps:     TestConnectorProps,
		Query:              "select index, prop1 from entity2",
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

func TestEvalBadTableName(t *testing.T) {

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

func TestEvalSelectWithSimpleWhere(t *testing.T) {

	settings := &Settings{
		URL:                TestUrl,
		UcsConnectionId:    TestUcsConnectionId,
		UcsConnectionToken: TestUcsConnectionToken,
		ConnectorName:      TestConnectorName,
		ConnectorProps:     TestConnectorProps,
		Query:              "select index, prop1 from entity2 where index < 10",
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
	assert.True(t, eof == true)

	results := tc.GetOutput("results").([]interface{})
	assert.True(t, len(results) == 10)
}

func TestEvalSelectWithWhereWithAnd(t *testing.T) {

	settings := &Settings{
		URL:                TestUrl,
		UcsConnectionId:    TestUcsConnectionId,
		UcsConnectionToken: TestUcsConnectionToken,
		ConnectorName:      TestConnectorName,
		ConnectorProps:     TestConnectorProps,
		Query:              "select * from entity2 where index < 10 and prop2 != 'xxxxxxx'",
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
	assert.True(t, eof == true)

	results := tc.GetOutput("results").([]interface{})
	assert.True(t, len(results) == 0) // looks to be a benchmark connector issue?
}

func TestEvalSelectWithWhereWithAndMixedCase(t *testing.T) {

	settings := &Settings{
		URL:                TestUrl,
		UcsConnectionId:    TestUcsConnectionId,
		UcsConnectionToken: TestUcsConnectionToken,
		ConnectorName:      TestConnectorName,
		ConnectorProps:     TestConnectorProps,
		Query:              "SELECT * FROM Entity2 WHERE Index < 10 AND Prop2 != 'xxxxxxx'",
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
	assert.True(t, eof == true)

	results := tc.GetOutput("results").([]interface{})
	assert.True(t, len(results) == 0) // looks to be a benchmark connector issue?
}

func TestEvalSelectWithWhereWithParam(t *testing.T) {

	settings := &Settings{
		URL:                TestUrl,
		UcsConnectionId:    TestUcsConnectionId,
		UcsConnectionToken: TestUcsConnectionToken,
		ConnectorName:      TestConnectorName,
		ConnectorProps:     TestConnectorProps,
		Query:              "select * from entity2 where index < :MaxIndex",
	}

	mf := mapper.NewFactory(resolve.GetBasicResolver())
	iCtx := test.NewActivityInitContext(settings, mf)
	act, err := New(iCtx)
	assert.Nil(t, err)

	tc := test.NewActivityContext(act.Metadata())

	params := map[string]interface{}{
		"MaxIndex": 42,
	}
	tc.SetInput("params", params)

	//eval
	done, err := act.Eval(tc)
	assert.True(t, done)
	assert.Nil(t, err)

	assert.NotNil(t, tc.GetOutput("eof"))
	assert.NotNil(t, tc.GetOutput("results"))

	eof := tc.GetOutput("eof").(bool)
	assert.True(t, eof == true)

	results := tc.GetOutput("results").([]interface{})
	assert.True(t, len(results) == 42)
}

func TestEvalSimpleSelectWithOrderBy(t *testing.T) {

	settings := &Settings{
		URL:                TestUrl,
		UcsConnectionId:    TestUcsConnectionId,
		UcsConnectionToken: TestUcsConnectionToken,
		ConnectorName:      TestConnectorName,
		ConnectorProps:     TestConnectorProps,
		Query:              "select * from entity2 orderby index",
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

	firstResult := results[0].(map[string]interface{})
	firstIndex := firstResult["Index"].(float64)
	assert.True(t, firstIndex == 1) // benchmark does not support orderby
}

func TestEvalSimpleSelectWithOrderByAsc(t *testing.T) {

	settings := &Settings{
		URL:                TestUrl,
		UcsConnectionId:    TestUcsConnectionId,
		UcsConnectionToken: TestUcsConnectionToken,
		ConnectorName:      TestConnectorName,
		ConnectorProps:     TestConnectorProps,
		Query:              "select * from entity2 orderby index asc",
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

	firstResult := results[0].(map[string]interface{})
	firstIndex := firstResult["Index"].(float64)
	assert.True(t, firstIndex == 1) // benchmark does not support orderby
}

func TestEvalSimpleSelectWithOrderByDesc(t *testing.T) {

	settings := &Settings{
		URL:                TestUrl,
		UcsConnectionId:    TestUcsConnectionId,
		UcsConnectionToken: TestUcsConnectionToken,
		ConnectorName:      TestConnectorName,
		ConnectorProps:     TestConnectorProps,
		Query:              "select * from entity2 orderby index desc",
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

	firstResult := results[0].(map[string]interface{})
	firstIndex := firstResult["Index"].(float64)
	assert.True(t, firstIndex == 1) // benchmark does not support orderby
}

func TestParseQuery(t *testing.T) {

	// basic select * query
	_, err := parseQuery("select * from entity2", nil)
	assert.Nil(t, err)

	// messy select * query
	_, err = parseQuery(" select  *  from  entity2 ", nil)
	assert.Nil(t, err)

	// select column query
	_, err = parseQuery("select index from entity2", nil)
	assert.Nil(t, err)

	// select columns query
	_, err = parseQuery("select index, prop1 from entity2", nil)
	assert.Nil(t, err)

	// select * query with top
	_, err = parseQuery("select top 100 * from entity2", nil)
	assert.Nil(t, err)

	// select * query with skip first
	_, err = parseQuery("select skip 100 * from entity2", nil)
	assert.Nil(t, err)

	// select * query with skip last
	_, err = parseQuery("select * from entity2 skip 100", nil)
	assert.Nil(t, err)

	// select * query with top and skip
	_, err = parseQuery("select top 100 skip 100 * from entity2", nil)
	assert.Nil(t, err)

	// select * query with where
	_, err = parseQuery("select * from entity2 where index < 5", nil)
	assert.Nil(t, err)

	// select * query with where with and and or
	_, err = parseQuery("select * from entity2 where index < 5 or prop1 == 'xxxxx'", nil)
	assert.Nil(t, err)

	// select * query with orderby
	_, err = parseQuery("select * from entity2 orderby index", nil)
	assert.Nil(t, err)

	// select * query with orderby asc
	_, err = parseQuery("select * from entity2 orderby index asc", nil)
	assert.Nil(t, err)

	// select * query with orderby desc
	_, err = parseQuery("select * from entity2 orderby index desc", nil)
	assert.Nil(t, err)

	// messy big fat pig query
	_, err = parseQuery(" Select top  100  skip  100  index , prop1 from  entity2 where index < 5 or prop1 == 'xxxxx'  orderby index   desc  ", nil)
	assert.Nil(t, err)

	// blank query
	_, err = parseQuery("", nil)
	assert.NotNil(t, err)

	// only select
	_, err = parseQuery("select", nil)
	assert.NotNil(t, err)

	// no columns
	_, err = parseQuery("select from entity2", nil)
	assert.NotNil(t, err)

	// no from
	_, err = parseQuery("select *", nil)
	assert.NotNil(t, err)

	// no table
	_, err = parseQuery("select * from", nil)
	assert.NotNil(t, err)

	// no where values
	_, err = parseQuery("select * from entity2 where", nil)
	assert.NotNil(t, err)

	// no orderby values
	_, err = parseQuery("select * from entity2 orderby", nil)
	assert.NotNil(t, err)
}

func TestBuildWherePart(t *testing.T) {

	// valid
	_, err := buildWherePart("a", "=", "b", "")
	assert.Nil(t, err)

	_, err = buildWherePart("a", "==", "b", "")
	assert.Nil(t, err)

	_, err = buildWherePart("a", "!=", "b", "")
	assert.Nil(t, err)

	_, err = buildWherePart("a", "<>", "b", "")
	assert.Nil(t, err)

	_, err = buildWherePart("a", ">", "b", "")
	assert.Nil(t, err)

	_, err = buildWherePart("a", "<", "b", "")
	assert.Nil(t, err)

	_, err = buildWherePart("a", ">=", "b", "")
	assert.Nil(t, err)

	_, err = buildWherePart("a", "<=", "b", "")
	assert.Nil(t, err)

	_, err = buildWherePart("a", "!>", "b", "")
	assert.Nil(t, err)

	_, err = buildWherePart("a", "!<", "b", "")
	assert.Nil(t, err)

	_, err = buildWherePart("a", "=", "b", "and")
	assert.Nil(t, err)

	_, err = buildWherePart("a", "=", "b", "or")
	assert.Nil(t, err)

	// invalid
	_, err = buildWherePart("", "", "", "")
	assert.NotNil(t, err)

	_, err = buildWherePart("a", "", "", "")
	assert.NotNil(t, err)

	_, err = buildWherePart("a", "=", "", "")
	assert.NotNil(t, err)

	_, err = buildWherePart("a", "", "b", "")
	assert.NotNil(t, err)

	_, err = buildWherePart("a", "??", "b", "")
	assert.NotNil(t, err)

	_, err = buildWherePart("a", "=", "b", "??")
	assert.NotNil(t, err)
}

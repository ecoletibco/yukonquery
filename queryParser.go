package yukonquery

import (
	"fmt"
	"strings"
)

const (
	SELECT    = "select"
	ALL       = "*"
	TOP       = "top"
	SKIP      = "skip"
	FROM      = "from"
	WHERE     = "where"
	ORDERBY   = "orderby"
	ASCENDING = "asc"
	DECENDING = "desc"
)

const (
	EQUAL            = "eq"
	NOT_EQUAL        = "ne"
	GREATER          = "gt"
	GREATER_OR_EQUAL = "ge"
	LESSER           = "lt"
	LESSER_OR_EQUAL  = "le"
)

const (
	AND = "and"
	OR  = "or"
	NOT = "not"
)

type Query struct {
	ConnectionId    string
	ConnectionToken string
	Select          string
	Top             string
	Skip            string
	From            string
	Where           string
	Orderby         string
}

func parseQuery(queryString string) (Query, error) {

	var queryObj = Query{}

	queryString = strings.ReplaceAll(queryString, ",", " ")
	queryString = strings.TrimSpace(queryString)
	queryString = strings.ToLower(queryString)
	if queryString == "" {
		return queryObj, fmt.Errorf("'query' is required")
	}

	selectIndex := -1
	topIndex := -1
	skipIndex := -1
	fromIndex := -1
	whereIndex := -1
	orderbyIndex := -1

	queryParts := strings.Split(queryString, " ")
	for i, queryPart := range queryParts {
		switch queryPart {
		case SELECT:
			selectIndex = i
		case TOP:
			topIndex = i
		case SKIP:
			skipIndex = i
		case FROM:
			fromIndex = i
		case WHERE:
			whereIndex = i
		case ORDERBY:
			orderbyIndex = i
		}
	}

	if selectIndex != 0 {
		return queryObj, fmt.Errorf("invalid query: only select statements are supported")
	}

	if fromIndex == -1 {
		return queryObj, fmt.Errorf("invalid query: a from clause is required")
	}

	if fromIndex+1 >= len(queryParts) {
		return queryObj, fmt.Errorf("invalid query: table name is required")
	}

	if topIndex != -1 {
		return queryObj, fmt.Errorf("invalid query: top not supported")
	}

	if skipIndex != -1 {
		return queryObj, fmt.Errorf("invalid query: skip not supported")
	}

	if whereIndex != -1 {
		return queryObj, fmt.Errorf("invalid query: where not supported")
	}

	if orderbyIndex != -1 {
		return queryObj, fmt.Errorf("invalid query: orderby not supported")
	}

	// parse for column names
	columnNames := ""
	subParts := queryParts[selectIndex+1:]
	for _, queryPart := range subParts {
		if queryPart == "" {
			continue
		} else if queryPart == ALL {
			columnNames = ALL
			break
		} else if queryPart == TOP {
			break
		} else if queryPart == SKIP {
			break
		} else if queryPart == FROM {
			break
		} else if queryPart == WHERE {
			break
		} else if queryPart == ORDERBY {
			break
		} else {
			if columnNames != "" {
				columnNames += ", "
			}
			columnNames += queryPart
		}
	}
	if columnNames == "" {
		return queryObj, fmt.Errorf("invalid query: select requires column list or * for all")
	}
	queryObj.Select = columnNames

	// parse for table name
	tableName := ""
	subParts = queryParts[fromIndex+1:]
	for _, queryPart := range subParts {
		if queryPart == "" {
			continue
		} else if queryPart == ALL {
			break
		} else if queryPart == TOP {
			break
		} else if queryPart == SKIP {
			break
		} else if queryPart == FROM {
			break
		} else if queryPart == WHERE {
			break
		} else if queryPart == ORDERBY {
			break
		} else {
			tableName = queryPart
		}
	}
	if tableName == "" {
		return queryObj, fmt.Errorf("invalid query: table name not found")
	}
	queryObj.From = tableName

	return queryObj, nil
}

package yukonquery

import (
	"fmt"
	"net/url"
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

var OpMap = map[string]string{
	"=":  "eq",
	"==": "eq",
	"!=": "ne",
	"<>": "ne",
	">":  "gt",
	">=": "ge",
	"!<": "ge",
	"<":  "lt",
	"<=": "le",
	"!>": "le",
}

const (
	AND = "and"
	OR  = "or"
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

func parseQuery(queryString string) (*Query, error) {

	queryString = strings.ReplaceAll(queryString, ",", " ")
	queryString = strings.TrimSpace(queryString)
	queryString = strings.ToLower(queryString)
	if queryString == "" {
		return nil, fmt.Errorf("'query' is required")
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
		return nil, fmt.Errorf("invalid query: only select statements are supported")
	}

	if fromIndex == -1 {
		return nil, fmt.Errorf("invalid query: a from clause is required")
	}

	if fromIndex+1 >= len(queryParts) {
		return nil, fmt.Errorf("invalid query: table name is required")
	}

	if topIndex != -1 {
		return nil, fmt.Errorf("invalid query: top not supported")
	}

	if skipIndex != -1 {
		return nil, fmt.Errorf("invalid query: skip not supported")
	}

	if orderbyIndex != -1 {
		return nil, fmt.Errorf("invalid query: orderby not supported")
	}

	var queryObj = Query{}

	columnNames, err := getColumnNames(queryParts[selectIndex+1:])
	if err != nil {
		return nil, err
	}
	queryObj.Select = url.QueryEscape(columnNames)

	tableName, err := getTableName(queryParts[fromIndex+1:])
	if err != nil {
		return nil, err
	}
	queryObj.From = url.QueryEscape(tableName)

	if whereIndex != -1 {
		where, err := getWhere(queryParts[whereIndex+1:])
		if err != nil {
			return nil, err
		}
		queryObj.Where = url.QueryEscape(where)
	}

	return &queryObj, nil
}

func getColumnNames(subParts []string) (string, error) {

	columnNames := ""
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
		return "", fmt.Errorf("invalid query: select requires column list or * for all")
	}
	return columnNames, nil
}

func getTableName(subParts []string) (string, error) {

	tableName := ""
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
		return "", fmt.Errorf("invalid query: table name not found")
	}
	return tableName, nil
}

func getWhere(subParts []string) (string, error) {

	where := ""

	left := ""
	op := ""
	right := ""
	logicOp := ""

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
			if left == "" {
				left = queryPart
			} else if op == "" {
				op = queryPart
			} else if right == "" {
				right = queryPart
			} else {
				logicOp = queryPart

				wherePart, err := buildWherePart(left, op, right, logicOp)
				if err != nil {
					return "", err
				}

				where += wherePart

				left = ""
				op = ""
				right = ""
				logicOp = ""
			}
		}
	}

	if left != "" {
		wherePart, err := buildWherePart(left, op, right, logicOp)
		if err != nil {
			return "", err
		}

		where += wherePart
	}

	where = strings.TrimSpace(where)

	if where == "" {
		return "", fmt.Errorf("invalid query: empty where clause")
	}
	return where, nil
}

func buildWherePart(left string, op string, right string, logicOp string) (string, error) {

	if left == "" || op == "" || right == "" {
		return "", fmt.Errorf("invalid query: invalid where clause '%s %s %s'", left, op, right)
	}

	opStr, ok := OpMap[op]
	if ok == false {
		return "", fmt.Errorf("invalid query: unknown operator '%s %s %s'", left, op, right)
	}

	if logicOp != "" && logicOp != AND && logicOp != OR {
		return "", fmt.Errorf("invalid query: unknown logical operator '%s'", logicOp)
	}

	return fmt.Sprintf("%s %s %s %s ", left, opStr, right, logicOp), nil
}

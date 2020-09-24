package yukonquery

import (
	"fmt"
	"strings"

	"github.com/project-flogo/core/data/coerce"
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
	Select  string
	Top     string
	Skip    string
	From    string
	Where   string
	Orderby string
}

func parseQuery(queryString string, params map[string]interface{}) (*Query, error) {

	queryString = strings.ReplaceAll(queryString, ",", " ")
	queryString = strings.TrimSpace(queryString)
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
		switch strings.ToLower(queryPart) {
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
	queryObj.Select = columnNames

	tableName, err := getTableName(queryParts[fromIndex+1:])
	if err != nil {
		return nil, err
	}
	queryObj.From = tableName

	if whereIndex != -1 {
		where, err := getWhere(queryParts[whereIndex+1:])
		if err != nil {
			return nil, err
		}

		for param, value := range params {
			strParam := ":" + param
			strValue, _ := coerce.ToString(value)
			originalWhere := where
			where = strings.ReplaceAll(where, strParam, strValue)
			if where == originalWhere {
				return nil, fmt.Errorf("invalid query: input param '%s' not found in query", param)
			}
		}

		queryObj.Where = where
	}

	return &queryObj, nil
}

func isDelimiter(queryPart string) bool {

	lowerQueryPart := strings.ToLower(queryPart)

	return lowerQueryPart == SELECT ||
		lowerQueryPart == TOP ||
		lowerQueryPart == SKIP ||
		lowerQueryPart == FROM ||
		lowerQueryPart == WHERE ||
		lowerQueryPart == ORDERBY
}

func getColumnNames(subParts []string) (string, error) {

	columnNames := ""
	for _, queryPart := range subParts {
		if queryPart == "" {
			continue
		} else if queryPart == ALL {
			columnNames = ALL
			break
		} else if isDelimiter(queryPart) {
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
		} else if isDelimiter(queryPart) {
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
		} else if isDelimiter(queryPart) {
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

	opStr, ok := OpMap[strings.ToLower(op)]
	if ok == false {
		return "", fmt.Errorf("invalid query: unknown operator '%s %s %s'", left, op, right)
	}

	lowerLogicOp := strings.ToLower(logicOp)
	if lowerLogicOp != "" && lowerLogicOp != AND && lowerLogicOp != OR {
		return "", fmt.Errorf("invalid query: unknown logical operator '%s'", logicOp)
	}

	return fmt.Sprintf("%s %s %s %s ", left, opStr, right, lowerLogicOp), nil
}

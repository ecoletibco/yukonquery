
# Yukon Query Activity 
This activity allows your Flogo application to execute queries using Yukon(Scribe) connectors. 


## Installation

```bash
flogo install github.com/ecoletibco/yukonquery
```

## Configuration

### Settings:
| Name               | Type   | Description
|:---                | :---   | :---    
| url                | string | The url of the Yukon server - **REQUIRED**  
| ucsConnectionId    | string | The Id of an existing USC connection, required for USC connections 
| ucsConnectionToken | string | The Auth Token to be used for the USC connection, required for USC connections        
| connectorName      | string | The Connector name, required for native Yukon connections 
| connectorProps     | map    | The connection properties to be used for the connection, required for native Yukon connections
| query              | string | The SQL select query - **REQUIRED**

### Input:
| Name   | Type | Description
|:---    | :--- | :---    
| params | map  |  The query parameters

### Output:
| Name        | Type  | Description
|:---         | :---  | :---    
| eof         | bool  |  False if more data is available
| results     | array |  The results

## Examples

### Query
Simple query that gets all items with ID less than 10, retrieves all the columns.  In order to use *mysql*, you have to import the driver by adding `github.com/go-sql-driver/mysql` to 
the app imports section.  See [github.com/go-sql-driver/mysql](https://github.com/go-sql-driver/mysql) for more information on the driver.
```json
{
  "id": "yukonquery",
  "name": "YukonQuery",
  "activity": {
    "ref": "github.com/ecoletibco/yukonquery",
    "settings": {
      "url": "https://localhost:44346/api",
      "ucsConnectionId": "ec296a0e-34f3-4c8a-8f83-3daf11c0913e",
      "ucsConnectionToken": "b40b3c7f-bfe5-4f41-aabc-36b086aae1fc",
      "query": "select * from test where ID < 10"
    }
  }
}
```

### Named Query
Query with parameters.  Parameters are referenced using ':', e.g. `:id`, regardless of database
```json
{
  "id": "yukonquery",
  "name": "YukonQuery",
  "activity": {
    "ref": "github.com/ecoletibco/yukonquery",
    "settings": {
      "url": "https://localhost:44346/api",
      "ucsConnectionId": "ec296a0e-34f3-4c8a-8f83-3daf11c0913e",
      "ucsConnectionToken": "b40b3c7f-bfe5-4f41-aabc-36b086aae1fc",
      "query": "select * from test where ID < :id"
    },
    "input": {
      "params": {
        "id":10
      }
    }
  }
}
```



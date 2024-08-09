package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net/url"
	"reflect"
	"slices"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gospider007/gson"
)

type ClientOption struct {
	DriverName  string //驱动名称
	OpenUrl     string //自定义的uri
	Usr         string //用户名
	Pwd         string //密码
	Host        string
	Port        int
	DbName      string            //数据库
	Protocol    string            //协议
	MaxConns    int               //最大连接数
	MaxLifeTime time.Duration     //最大活跃数
	Params      map[string]string //附加参数
}
type Client struct {
	db *sql.DB
}
type Rows struct {
	rows  *sql.Rows
	names []string
	kinds []reflect.Type
}
type Result struct {
	result sql.Result
}

// 新插入的列
func (obj *Result) LastInsertId() (int64, error) {
	return obj.result.LastInsertId()
}

// 受影响的行数
func (obj *Result) RowsAffected() (int64, error) {
	return obj.result.RowsAffected()
}

// 是否有下一个数据
func (obj *Rows) Next() bool {
	if obj.rows.Next() {
		return true
	} else {
		obj.Close()
		return false
	}
}

type AnyValue interface {
	Value() (driver.Value, error)
}

// 返回游标的数据
func (obj *Rows) Data() (map[string]any, error) {
	result := make([]any, len(obj.kinds))
	for k, v := range obj.kinds {
		result[k] = reflect.New(v).Interface()
	}
	err := obj.rows.Scan(result...)
	if err != nil {
		return nil, err
	}
	maprs := map[string]any{}
	for k, v := range obj.names {
		val := reflect.ValueOf(result[k]).Elem().Interface()
		anyVal, ok := val.(AnyValue)
		if ok {
			value, err := anyVal.Value()
			if err != nil {
				return maprs, err
			}
			maprs[v] = value
		} else {
			maprs[v] = val
		}
	}
	return maprs, nil
}

// 关闭游标
func (obj *Rows) Close() error {
	return obj.rows.Close()
}

func NewClient(ctx context.Context, options ...ClientOption) (*Client, error) {
	var option ClientOption
	if len(options) > 0 {
		option = options[0]
	}
	if ctx == nil {
		ctx = context.TODO()
	}
	if option.DriverName == "" {
		option.DriverName = "mysql"
	}
	if option.MaxConns == 0 {
		option.MaxConns = 65535
	}
	var openAddr string
	if option.OpenUrl != "" {
		openAddr = option.OpenUrl
	} else {
		if option.Usr != "" {
			if option.Pwd != "" {
				openAddr += fmt.Sprintf("%s:%s@", option.Usr, option.Pwd)
			} else {
				openAddr += fmt.Sprintf("%s@", option.Usr)
			}
		}
		if option.Protocol != "" {
			openAddr += option.Protocol
		}
		if option.Host != "" {
			if option.Port == 0 {
				openAddr += fmt.Sprintf("(%s)/", option.Host)
			} else {
				openAddr += fmt.Sprintf("(%s:%d)/", option.Host, option.Port)
			}
		}
		if option.DbName != "" {
			openAddr += option.DbName
		}
		if option.Params != nil && len(option.Params) > 0 {
			value := url.Values{}
			for k, v := range option.Params {
				value.Add(k, v)
			}
			openAddr += "?" + value.Encode()
		}
	}
	db, err := sql.Open(option.DriverName, openAddr)
	if err != nil {
		return nil, err
	}
	err = db.PingContext(ctx)
	if err != nil {
		return nil, err
	}
	db.SetConnMaxIdleTime(option.MaxLifeTime)
	db.SetConnMaxLifetime(option.MaxLifeTime)
	db.SetMaxOpenConns(option.MaxConns)
	db.SetMaxIdleConns(option.MaxConns)
	return NewClientWithSqlDB(db), nil
}

func NewClientWithSqlDB(db *sql.DB) *Client {
	return &Client{db: db}
}

func (obj *Client) Insert(ctx context.Context, table string, data ...any) (*Result, error) {
	if ctx == nil {
		ctx = context.TODO()
	}
	names, indexs, values, err := obj.parseInserts(data...)
	if err != nil {
		return nil, err
	}
	return obj.Exec(ctx, fmt.Sprintf("insert ignore into %s %s values %s", table, names, indexs), values...)
}
func (obj *Client) InsertWithValues(ctx context.Context, table string, data ...[]any) (*Result, error) {
	if ctx == nil {
		ctx = context.TODO()
	}
	indexs, values := obj.parseInsertWithValues(data...)
	return obj.Exec(ctx, fmt.Sprintf("insert ignore into %s values %s;", table, indexs), values...)
}
func (obj *Client) parseInsert(data map[string]any, keys []string) (string, []any) {
	values := []any{}
	indexs := make([]string, len(keys))
	for i, k := range keys {
		v := data[k]
		values = append(values, v)
		indexs[i] = "?"
	}
	return fmt.Sprintf("(%s)", strings.Join(indexs, ", ")), values
}
func (obj *Client) parseInsertWithValues(values ...[]any) (string, []any) {
	indexs := make([]string, len(values))
	vvs := []any{}
	for i, vs := range values {
		index := make([]string, len(vs))
		for j, v := range vs {
			index[j] = "?"
			vvs = append(vvs, v)
		}
		indexs[i] = fmt.Sprintf("(%s)", strings.Join(index, ", "))
	}
	return strings.Join(indexs, ", "), vvs
}

func (obj *Client) parseInsertKeys(data ...any) ([]map[string]any, []string, error) {
	values := []map[string]any{}
	keys := []string{}
	for _, d := range data {
		jsonData, err := gson.Decode(d)
		if err != nil {
			return nil, nil, err
		}
		value := map[string]any{}
		for key, val := range jsonData.Map() {
			value[key] = val.Value()
			if !slices.Contains(keys, key) {
				keys = append(keys, key)
			}
		}
		values = append(values, value)
	}

	return values, keys, nil
}
func (obj *Client) parseInserts(data ...any) (string, string, []any, error) {
	datas, names, err := obj.parseInsertKeys(data...)
	if err != nil {
		return "", "", nil, err
	}
	values := []any{}
	indexs := []string{}

	for _, d := range datas {
		index, value := obj.parseInsert(d, names)
		indexs = append(indexs, index)
		values = append(values, value...)
	}
	return fmt.Sprintf("(%s)", strings.Join(names, ", ")), strings.Join(indexs, ", "), values, nil
	// return fmt.Sprintf("(%s)", strings.Join(names, ", ")), fmt.Sprintf("(%s)", strings.Join(indexs, ", ")), values, nil
}

// finds   ?  is args
func (obj *Client) Finds(ctx context.Context, query string, args ...any) (*Rows, error) {
	if ctx == nil {
		ctx = context.TODO()
	}
	row, err := obj.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	cols, err := row.ColumnTypes()
	if err != nil {
		return nil, err
	}
	names := make([]string, len(cols))
	kinds := make([]reflect.Type, len(cols))
	for coln, col := range cols {
		names[coln] = col.Name()
		kinds[coln] = col.ScanType()
	}
	return &Rows{
		names: names,
		kinds: kinds,
		rows:  row,
	}, err
}

// 执行
func (obj *Client) Exec(ctx context.Context, query string, args ...any) (*Result, error) {
	if ctx == nil {
		ctx = context.TODO()
	}
	exeResult, err := obj.db.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &Result{result: exeResult}, nil
}

// 关闭客户端
func (obj *Client) Close() error {
	return obj.db.Close()
}

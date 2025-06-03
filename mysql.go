package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"iter"
	"net"
	"net/url"
	"reflect"
	"slices"
	"strings"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/gospider007/gson"
	"github.com/gospider007/gtls"
	"github.com/gospider007/requests"
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
	Socks5Proxy string
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

// 遍历
func (obj *Rows) Range() iter.Seq[map[string]any] {
	return func(yield func(map[string]any) bool) {
		defer obj.Close()
		for obj.Next() {
			if !yield(obj.Map()) {
				return
			}
		}
	}
}

type AnyValue interface {
	Value() (driver.Value, error)
}

// 返回游标的数据
func (obj *Rows) Map() map[string]any {
	result := make([]any, len(obj.kinds))
	for k, v := range obj.kinds {
		result[k] = reflect.New(v).Interface()
	}
	err := obj.rows.Scan(result...)
	if err != nil {
		obj.Close()
		return nil
	}
	maprs := map[string]any{}
	for k, v := range obj.names {
		val := reflect.ValueOf(result[k]).Elem().Interface()
		anyVal, ok := val.(AnyValue)
		if ok {
			value, err := anyVal.Value()
			if err != nil {
				obj.Close()
				return maprs
			}
			maprs[v] = value
		} else {
			maprs[v] = val
		}
	}
	return maprs
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
	if option.Socks5Proxy != "" {
		proxy, err := gtls.VerifyProxy(option.Socks5Proxy)
		if err != nil {
			return nil, err
		}
		proxyAddress, err := requests.GetAddressWithUrl(proxy)
		if err != nil {
			return nil, err
		}
		if proxyAddress.Scheme != "socks5" {
			return nil, fmt.Errorf("only support socks5 proxy")
		}
		dialer := &requests.Dialer{}
		mysqlDriver.RegisterDialContext("tcp", func(ctx context.Context, addr string) (net.Conn, error) {
			remoteAdress, err := requests.GetAddressWithAddr(addr)
			if err != nil {
				return nil, err
			}
			return dialer.Socks5TcpProxy(requests.NewResponse(ctx, requests.RequestOption{}), proxyAddress, remoteAdress)
		})
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
		if len(option.Params) > 0 {
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
	return obj.Exec(ctx, fmt.Sprintf("insert into %s %s values %s", table, names, indexs), values...)
}
func (obj *Client) InsertWithValues(ctx context.Context, table string, data ...[]any) (*Result, error) {
	if ctx == nil {
		ctx = context.TODO()
	}
	indexs, values := obj.parseInsertWithValues(data...)
	return obj.Exec(ctx, fmt.Sprintf("insert into %s values %s;", table, indexs), values...)
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

// 执行
func (obj *Client) Update(ctx context.Context, table string, data any, where string, args ...any) (*Result, error) {
	if ctx == nil {
		ctx = context.TODO()
	}
	if where == "" {
		return nil, fmt.Errorf("where is empty")
	}
	jsonData, err := gson.Decode(data)
	if err != nil {
		return nil, err
	}
	names := []string{}
	values := []any{}
	for key, val := range jsonData.Map() {
		names = append(names, fmt.Sprintf("%s=?", key))
		values = append(values, val.Value())
	}
	exeResult, err := obj.db.ExecContext(ctx, fmt.Sprintf("update %s set %s where %s", table, strings.Join(names, ", "), where), append(values, args...)...)
	if err != nil {
		return nil, err
	}
	return &Result{result: exeResult}, nil
}

// 执行
func (obj *Client) Exists(ctx context.Context, table string, where string, args ...any) (bool, error) {
	if ctx == nil {
		ctx = context.TODO()
	}
	if where == "" {
		return false, fmt.Errorf("where is empty")
	}
	exeResult, err := obj.Finds(ctx, fmt.Sprintf("select 1 from %s where %s limit 1", table, where), args...)
	if err != nil {
		return false, err
	}
	var exists bool
	for range exeResult.Range() {
		exists = true
		break
	}
	return exists, nil
}

// 执行
func (obj *Client) UpSert(ctx context.Context, table string, data any, where string, args ...any) (*Result, error) {
	if ctx == nil {
		ctx = context.TODO()
	}
	results, err := obj.Update(ctx, table, data, where, args...)
	if err != nil {
		return nil, err
	}
	i, err := results.RowsAffected()
	if err != nil {
		return nil, err
	}
	if i == 0 {
		exists, err := obj.Exists(ctx, table, where, args...)
		if err != nil {
			return nil, err
		}
		if !exists {
			return obj.Insert(ctx, table, data)
		}
	}
	return results, nil
}

// 关闭客户端
func (obj *Client) Close() error {
	return obj.db.Close()
}

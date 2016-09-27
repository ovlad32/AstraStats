package main

import (
	"bufio"
	"compress/gzip"
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/lib/pq"
	"io"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"runtime"
	"time"
)

type ConfigType struct {
	LineSeparator  int `json:"lineSeparatorByte"`
	FieldSeparator int `json:"fieldSeparatorByte"`
}

var (
	Db   *sql.DB
	Conf ConfigType
)

type ColumnDataImage struct {
	RowNumber  uint64
	Image      string
	ColumnInfo *ColumnInfoType
	Value      interface{}
}
type MessageType map[string]interface{}

func NewMessage() *MessageType {
	r := make(MessageType)
	return &r
}
func (m *MessageType) Add(key string, data interface{}) *MessageType {
	(*m)[key] = data
	return m
}

/*
func (m *MessageType) Done() MessageType {
       return (*m)
}*/

type MessageChannel chan *MessageType

func NewMessageChannel() MessageChannel {
	return make(MessageChannel)
}

func NewMessageChannelSize(size int) MessageChannel {
	return make(MessageChannel, size)
}

type TableInfoType struct {
	Id            sql.NullInt64
	DatabaseName  sql.NullString
	SchemaName    sql.NullString
	Name          sql.NullString
	RowCount      sql.NullInt64
	Dumped        sql.NullString
	Indexed       sql.NullString
	PathToFile    sql.NullString
	PathToDataDir sql.NullString
	Columns       []ColumnInfoType
}
type ColumnInfoType struct {
	Id              sql.NullInt64
	Name            sql.NullString
	DataLength      sql.NullInt64
	DataPrecision   sql.NullInt64
	DataScale       sql.NullInt64
	DataType        sql.NullInt64
	HashUniqueCount sql.NullInt64
	UniqueRowCount  sql.NullInt64
	TotalRowCount   sql.NullInt64
	Position        sql.NullInt64
	RealType        sql.NullString
	TableInfo       *TableInfoType
	//-------------------
	IsNumeric    bool
	IsInteger    bool
	IntegerMin   int64
	IntegerMax   int64
	FloatMin     float64
	FloatMax     float64
	NullCount    int64
	NumericCount uint64
	// -------------------
}

func DbInit(dbname string) {
	var err error
	//pwd for sbs:123123
	Db, err = sql.Open("postgres", "user=edm password=edmedm dbname="+dbname+" host=localhost port=5435 sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
}

func varInit() {
	props, err := os.Open("AstraStats.json")
	if err != nil {
		panic(err)
	}
	defer props.Close()
	dec := json.NewDecoder(props)

	err = dec.Decode(&Conf)
	if err != nil {
		panic(err)
	}
	//fmt.Println(Conf)
}
func main() {
	now := time.Now()
	var id int64
	runtime.GOMAXPROCS(4)
	if len(os.Args) < 3 {
		panic("AstraTableStats dbName #table_id!")
	}
	//fmt.Println(os.Args,len(os.Args))
	dbName := os.Args[1]

	id, err := strconv.ParseInt(os.Args[2], 10, 64)
	if err != nil {
		panic("table id is not numeric! " + err.Error())
	}

	varInit()
	DbInit(dbName)

	_, err = Db.Exec("create table if not exists column_stats(id bigint not null,imin bigint,imax bigint,fmax float,fmin float, constraint column_stats_pk primary key (id))")

	tiRws, err := Db.Query(fmt.Sprintf(
		"select "+
			" ID"+
			" ,DATABASE_NAME"+
			" ,SCHEMA_NAME"+
			" ,NAME"+
			" ,ROW_COUNT"+
			" ,DUMPED"+
			" ,PATH_TO_FILE"+
			" ,PATH_TO_DATA_DIR"+
			" from TABLE_INFO t where t.ID = %v", id))
	if err != nil {
		panic(err)
	}

	var ti TableInfoType

	start := NewMessageChannel()
	tableInfoC1 := NewMessageChannel()
	finish := NewMessageChannel()
	statsChan := NewMessageChannelSize(1000)
	saveChan := NewMessageChannelSize(1000)
	fileChan := NewMessageChannelSize(1000)
	//doneChan2 := NewMessageChannelSize(1000)
	go CreateColumnTable(start, tableInfoC1)
	go PipeTableData(tableInfoC1, statsChan)
	go CollectStats(statsChan, saveChan)
	go SaveData(saveChan, fileChan)
	go Terminator(fileChan, finish)
	//go LoadH2(fileChan, doneChan2)
	//go Terminator(doneChan2, finish)

	if !tiRws.Next() {
		panic(fmt.Sprintf("There is no table having id = %v", id))
	} else {
		tiRws.Scan(
			&ti.Id,
			&ti.DatabaseName,
			&ti.SchemaName,
			&ti.Name,
			&ti.RowCount,
			&ti.Dumped,
			&ti.PathToFile,
			&ti.PathToDataDir,
		)
		ti.Columns = make([]ColumnInfoType, 0, 10)

		ciRws, err := Db.Query(fmt.Sprintf(
			"select"+
				" ID"+
				" ,NAME"+
				" ,DATA_TYPE"+
				" ,REAL_TYPE"+
				" ,CHAR_LENGTH"+
				" ,DATA_PRECISION"+
				" ,DATA_SCALE"+
				" ,POSITION"+
				" ,TOTAL_ROW_COUNT"+
				" ,UNIQUE_ROW_COUNT"+
				" ,HASH_UNIQUE_COUNT"+
				" from column_info c " +
				" where c.TABLE_INFO_ID = %v " +
				" order by c.POSITION asc", id))
		if err != nil {
			panic(err)
		}
		fmt.Println(ti.Name.String)

		for ciRws.Next() {
			var ci ColumnInfoType
			ciRws.Scan(
				&ci.Id,
				&ci.Name,
				&ci.DataType,
				&ci.RealType,
				&ci.DataLength,
				&ci.DataPrecision,
				&ci.DataScale,
				&ci.Position,
				&ci.TotalRowCount,
				&ci.UniqueRowCount,
				&ci.HashUniqueCount,
			)
			ci.IsNumeric = false
			ci.IsInteger = true
			ci.NullCount = 0
			ci.FloatMin, ci.FloatMax = math.MaxFloat64, -math.MaxFloat64
			ci.IntegerMin, ci.IntegerMax = math.MaxInt64, math.MinInt64
			ci.TableInfo = &ti
			ti.Columns = append(ti.Columns, ci)

		}
		//fmt.Println(ti)
		start <- NewMessage().Add("table", ti)

	}

	<-finish

	for index := range ti.Columns {
		c := ti.Columns[index]
		if c.IsNumeric {
			fmt.Print(c.Name.String)
			Db.Exec(fmt.Sprintf("delete from column_stats where id=%v", c.Id.Int64))

			if c.IsInteger {
				fmt.Println(" Integer:", c.IntegerMin, " - ", c.IntegerMax, " count:", c.NumericCount)
				Db.Exec(fmt.Sprintf("Insert into column_stats(id,imin,imax) values(%v,%v,%v)", c.Id.Int64, c.IntegerMin, c.IntegerMax))
			} else {
				fmt.Print(" Float:", c.FloatMin, " - ", c.FloatMax, " count:", c.NumericCount)
				Db.Exec(fmt.Sprintf("Insert into column_stats(id,fmin,fmax) values(%v,%v,%v)", c.Id.Int64, c.FloatMin, c.FloatMax))
			}
		}

	}

	Db.Close()
	fmt.Println("Took ",time.Since(now))

}
func ColumnTableName(column *ColumnInfoType) string {
	return fmt.Sprintf("%v_$%v",
		column.TableInfo.Name.String,
		column.Name.String,
	)
}
func CreateColumnTable(in MessageChannel, out MessageChannel) {
	for msg := range in {
		if raw, found := (*msg)["table"]; found {

			ti := raw.(TableInfoType)
			for _, column := range ti.Columns {
				tableName := ColumnTableName(&column)

				tx, err := Db.Begin()
				if err != nil {
					panic(err)
				}

				_, err = tx.Exec(fmt.Sprintf("drop table if exists %v", tableName))
				if err != nil {
					panic(err)
				}

				_, err = tx.Exec(fmt.Sprintf("create table if not exists %v (col float,cnt bigint)", tableName))
				if err != nil {
					panic(err)
				}

				err = tx.Commit()
				if err != nil {
					// AutoCommitMode
					//panic(err)
				}
			}
			out <- msg
		}
	}
	close(out)
}

func PipeTableData(in MessageChannel, out MessageChannel) {
	for msg := range in {
		ti := (*msg)["table"].(TableInfoType)
		gzfile, err := os.Open(ti.PathToFile.String)
		if err != nil {
			panic(err)
		}

		defer gzfile.Close()

		file, err := gzip.NewReader(gzfile)
		if err != nil {
			panic(err)
		}
		defer file.Close()

		rawData := bufio.NewReaderSize(file, 50*1024)

		sLineSeparator := string(Conf.LineSeparator)
		sFieldSeparator := string(Conf.FieldSeparator)
		metadataColumnCount := len(ti.Columns)
		lineNumber := uint64(0)
		for {
			line, err := rawData.ReadString(byte(Conf.LineSeparator))
			if err == io.EOF {
				close(out)
				break

			} else if err != nil {
				panic(err)
			}
			lineNumber++

			line = strings.TrimSuffix(line, sLineSeparator)
			line = strings.TrimSuffix(line, string(byte(0xD)))

			//            fmt.Print(line)
			lineColumns := strings.Split(line, sFieldSeparator)
			lineColumnCount := len(lineColumns)
			if metadataColumnCount != lineColumnCount {
				panic(fmt.Sprintf("Number of column mismatch in line %v. Expected #%v; Actual #%v",
					lineNumber,
					metadataColumnCount,
					lineColumnCount,
				))
			}

			for columnIndex := range ti.Columns {
				if columnIndex == 0 && lineNumber == 1 {
					out <- NewMessage().Add("table", ti)
				}
				out <- NewMessage().Add(
					"data",
					ColumnDataImage{
						ColumnInfo: &ti.Columns[columnIndex],
						Image:      lineColumns[columnIndex],
						RowNumber:  lineNumber,
					},
				)

			}

		}
	}

}

func CollectStats(in MessageChannel, out MessageChannel) {
	for msg := range in {
		if _, found := (*msg)["table"]; found {
			out <- msg
		} else if raw, found := (*msg)["data"]; found {

			data := raw.(ColumnDataImage)
			column := data.ColumnInfo
			sValue := data.Image

			if sValue == "" {
				continue
			}
			fval, err := strconv.ParseFloat(sValue, 64)
			if err != nil {
				continue
			}
			column.IsNumeric = true
			column.NumericCount++

			var ival int64
			if column.IsInteger {
				ival = int64(math.Trunc(fval))
				if float64(ival) != fval {

					column.IsInteger = false

					if float64(column.IntegerMax) > column.FloatMax {
						column.FloatMax = float64(column.IntegerMax)
						column.IntegerMax = 0
					}
					if float64(column.IntegerMin) < column.FloatMin {
						column.FloatMin = float64(column.IntegerMin)
						column.IntegerMin = 0
					}
				}
			}
			if column.IsInteger {
				if column.IntegerMax < ival {
					column.IntegerMax = ival
				}
				if column.IntegerMin > ival {
					column.IntegerMin = ival
				}
			} else {
				if column.FloatMax < fval {
					column.FloatMax = fval
				}
				if column.FloatMin > fval {
					column.FloatMin = fval
				}
			}
			if column.IsNumeric {
				out <- NewMessage().Add(
					"data",
					data,
				)
			}
		}
	}
	close(out)
}

func SaveData(in MessageChannel, out MessageChannel) {
	zip := true

	type Writer interface {
		Write(a []byte) (int, error)
	}
	type Closer interface {
		Close() error
	}

	type Dump struct {
		pathToFile string
		writer     Writer
		closer     Closer
		column     *ColumnInfoType
	}
	type Dumpers map[string]Dump
	var dumpers Dumpers

	PushDumperInfoToOutChannel := func() {
		for _, v := range dumpers {
			v.closer.Close()
			//fmt.Println(v.column.Name, v.pathToFile)
			out <- NewMessage().
				Add("column", v.column).
				Add("path", v.pathToFile)
		}
	}

	var sLineSeparator string = string(Conf.LineSeparator)
	for msg := range in {
		if _, found := (*msg)["table"]; found {
			if dumpers != nil {
				PushDumperInfoToOutChannel()
			}
			dumpers = make(Dumpers)
		} else if raw, found := (*msg)["data"]; found {
			data := raw.(ColumnDataImage)

			path := data.ColumnInfo.TableInfo.PathToDataDir.String +
				"/NUMERIC_DATA/" + data.ColumnInfo.TableInfo.Name.String + "/" +
				data.ColumnInfo.Name.String
			if data.ColumnInfo.IsInteger {
				path += "/I/"
			} else {
				path += "/F/"
			}
			name := fmt.Sprintf("%v", len(data.Image))

			fullFileName := path + name
			if dumper, found := dumpers[fullFileName]; !found {
				err := os.MkdirAll(path, 0)
				if err != nil {
					panic(err)
				}

				file, err := os.Create(fullFileName)
				if err != nil {
					panic(err)
				}
				dumper.column = data.ColumnInfo
				dumper.pathToFile = fullFileName
				if zip {
					gzipFile, err := gzip.NewWriterLevel(file, gzip.BestSpeed)
					if err != nil {
						panic(err)
					}
					//dumper.writer = bufio.NewWriterSize(gzipFile,4*32*1024)
					dumper.writer = gzipFile
					dumper.closer = gzipFile

				} else {
					dumper.closer = file
					dumper.writer = bufio.NewWriterSize(file,50*1024)
				}
				dumpers[fullFileName] = dumper

				dumper.writer.Write([]byte(data.Image + sLineSeparator))
			} else {
				dumper.writer.Write([]byte(data.Image + sLineSeparator))
			}
		}
	}
	if dumpers != nil {
		PushDumperInfoToOutChannel()
	}
	close(out)
}
func LoadH2(in MessageChannel, out MessageChannel) {
	curDir, _ := os.Getwd()
	for msg := range in {
		if rawPath, found := (*msg)["path"]; found {
			path := rawPath.(string)
			column := (*msg)["column"].(*ColumnInfoType)

			tableName := fmt.Sprintf("%v_$%v",
				column.TableInfo.Name.String,
				column.Name.String,
			)

			{
				tx, err := Db.Begin()
				if err != nil {
					panic(err)
				}
				sql := fmt.Sprintf("insert into %v select COL,count(1) as CNT from csvRead('%v','COL') group by COL",
					tableName, curDir+"/"+path)
				//fmt.Println(sql)

				_, err = tx.Exec(sql)
				if err != nil {
					panic(err)
				}
				err = tx.Commit()
				if err != nil {
					//AutoCommitMode
					//panic(err)
				}
			}

		}
	}
	close(out)
}

func Terminator(in MessageChannel, out MessageChannel) {
	for _ = range in {
	}
	out <- NewMessage()
}

//
// htps://godoc.org/github.com/cznic/lldb
// https://www.reddit.com/r/golang/comments/3m1xcu/embeddable_database_for_go/

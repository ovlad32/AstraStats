package main

import (
	"bufio"
	"compress/gzip"
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/lib/pq"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"io/ioutil"
	"path"
	"path/filepath"
	"log"
	"sort"
)

type ConfigType struct {
	AppMode        string
	DumpRootPath   string `json:"dumpRootPath"`
	LineSeparator  int    `json:"lineSeparatorByte"`
	FieldSeparator int    `json:"fieldSeparatorByte"`
	H2Host         string `json:"h2-host"`
	H2Port         string `json:"h2-port"`
	H2Name         string `json:"h2-dbname"`
	H2Login        string `json:"h2-login"`
	H2Password     string `json:"h2-password"`
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
	TableName     sql.NullString
	RowCount      sql.NullInt64
	Dumped        sql.NullString
	Indexed       sql.NullString
	PathToFile    sql.NullString
	PathToDataDir sql.NullString
	Columns       []ColumnInfoType
}
type ColumnInfoType struct {
	Id              sql.NullInt64
	ColumnName      sql.NullString
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

func(c ColumnInfoType) h2ColumnTableName() string {
	return fmt.Sprintf("%v_$%v",
		c.TableInfo.TableName.String,
		c.ColumnName.String,
	)
}

func (c ColumnInfoType) pathToNumericDump(conf *ConfigType) (string) {
	return conf.DumpRootPath + "/" +
		c.TableInfo.PathToDataDir.String + "/" +
		"NUMERIC_DATA/" +
		c.TableInfo.TableName.String + "/" +
		c.ColumnName.String + "/"
}

func DbInit() {
	var err error
	//pwd for sbs:123123
	params := fmt.Sprintf("host=%v port=%v dbname=%v user=%v password=%v sslmode=disable",
		Conf.H2Host,
		Conf.H2Port,
		Conf.H2Name,
		Conf.H2Login,
		Conf.H2Password,
	)
	//fmt.Println(params)
	Db, err = sql.Open("postgres", params)

	if err != nil {
		panic(err)
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
	if Conf.DumpRootPath == "" {
		Conf.DumpRootPath = "."
	}
//	fmt.Println(Conf)
}
func main() {
	var id int64
	if len(os.Args) < 3 {
		panic("AstraTableStats mode #table_id!")
	}
	//fmt.Println(os.Args,len(os.Args))
	mode := os.Args[1]

	id, err := strconv.ParseInt(os.Args[2], 10, 64)
	if err != nil {
		panic("table id is not numeric! " + err.Error())
	}

	varInit()
	DbInit()

	Conf.AppMode = mode

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
	finish := NewMessageChannel()

	switch strings.ToUpper(Conf.AppMode) {
	case strings.ToUpper("NumericExtract"):
		{
			saveChan := NewMessageChannelSize(1000)
			statsChan := NewMessageChannelSize(1000)
			fileChan := NewMessageChannelSize(1000)
			doneChan2 := NewMessageChannelSize(1000)
			go PipeTableData(start, statsChan)
			go CollectMinMaxStats(statsChan, saveChan)
			go SaveData(saveChan, fileChan)
			//go LoadH2(fileChan, doneChan2)
			go Terminator(doneChan2, finish)
		}
	case strings.ToUpper("Consolidation"):
		{
			statsChan := NewMessageChannelSize(1000)
			doneChan2 := NewMessageChannelSize(1000)
			go PipeDumpFileNames(start, statsChan)
			go FIConsolidation(statsChan,doneChan2)
			go Terminator(doneChan2, finish)

		}

	}

	if !tiRws.Next() {
		panic(fmt.Sprintf("There is no table having id = %v", id))
	} else {
		tiRws.Scan(
			&ti.Id,
			&ti.DatabaseName,
			&ti.SchemaName,
			&ti.TableName,
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
				" from column_info c where c.TABLE_INFO_ID = %v order by c.POSITION asc", id))
		if err != nil {
			panic(err)
		}

		for ciRws.Next() {
			var ci ColumnInfoType
			ciRws.Scan(
				&ci.Id,
				&ci.ColumnName,
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
	close(start)
	<-finish

	for index := range ti.Columns {
		c := ti.Columns[index]
		if c.IsNumeric {
			fmt.Print(c.ColumnName.String)
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

}


func CreateColumnTable(in MessageChannel, out MessageChannel) {
	for msg := range in {
		if raw, found := (*msg)["table"]; found {

			ti := raw.(TableInfoType)
			for _, column := range ti.Columns {
				tableName := column.h2ColumnTableName()

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
		gzfile, err := os.Open(Conf.DumpRootPath + ti.PathToFile.String)
		if err != nil {
			panic(err)
		}

		defer gzfile.Close()

		file, err := gzip.NewReader(gzfile)
		if err != nil {
			panic(err)
		}
		defer file.Close()

		rawData := bufio.NewReaderSize(file, 5*1024)

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

func CollectMinMaxStats(in MessageChannel, out MessageChannel) {
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
	zip := false

	type Writer interface {
		Write(a []byte) (int, error)
		Close() error
	}

	type Dump struct {
		pathToFile string
		writer     Writer
		column     *ColumnInfoType
	}
	type Dumpers map[string]Dump
	var dumpers Dumpers

	PushDumperInfoToOutChannel := func() {
		for _, v := range dumpers {
			v.writer.Close()
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

			pathToDump := data.ColumnInfo.pathToNumericDump(&Conf)

			if data.ColumnInfo.IsInteger {
				pathToDump += "/I/"
			} else {
				pathToDump += "/F/"
			}
			name := fmt.Sprintf("%v.raw.gz", len(data.Image))

			fullFileName := pathToDump + name
			if dumper, found := dumpers[fullFileName]; !found {
				err := os.MkdirAll(pathToDump, 0)
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
					dumper.writer, err = gzip.NewWriterLevel(file, gzip.BestSpeed)
					if err != nil {
						panic(err)
					}
				} else {
					dumper.writer = file
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
			pathTo := rawPath.(string)
			column := (*msg)["column"].(*ColumnInfoType)

			tableName := column.h2ColumnTableName()

			{
				tx, err := Db.Begin()
				if err != nil {
					panic(err)
				}
				pathTo := Conf.DumpRootPath+"/"+pathTo;

				if !path.IsAbs(pathTo) {
					pathTo = curDir + pathTo
				}

				sql := fmt.Sprintf("insert into %v select COL,count(1) as CNT from csvRead('%v','COL') group by COL",
					tableName, pathTo)
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
func PipeDumpFileNames(in MessageChannel, out MessageChannel) {

	for msg := range in {
		sent := make(map[string]bool)

		ti := (*msg)["table"].(TableInfoType)
		out <- msg
		for _,column := range ti.Columns {
			path := column.pathToNumericDump(&Conf)
			pathF := path+"F/"
			pathI := path+"I/"

			contentsI,err := ioutil.ReadDir(pathI)
			if err != nil {
				continue
			}
			for _, f := range contentsI {
				if !f.IsDir() {
					if _, err := os.Stat(pathF+f.Name()); os.IsNotExist(err) {
						out <- NewMessage().Add("I", pathI+f.Name())
						//fmt.Println(pathI+f.Name())
					}else{
						out<-NewMessage().Add("NI",pathI+f.Name())
						out<-NewMessage().Add("NF",pathF+f.Name())
						//fmt.Println(pathF+f.Name())
						//fmt.Println(pathI+f.Name())
						sent[f.Name()]=true
					}
				}
			}
			contentsF,err := ioutil.ReadDir(pathF)
			if err != nil {
				continue
			}
			for _, f := range contentsF {
				if !f.IsDir() {
					if _, found := sent[f.Name()]; !found {
						out <- NewMessage().Add("F", pathF+f.Name())
						fmt.Println(pathF+f.Name())

					}
				}
			}
		}
	}
	close(out)
}

type kvType struct {
	key string
	value string
}
type kvArrayType []kvType;
func(k kvArrayType) Len() int {return len(k)}
func (k kvArrayType) Less(i, j int) bool {
	ki,_ := strconv.ParseFloat(k[i].key,64);
	kj,_ := strconv.ParseFloat(k[j].key,64);
	return ki < kj
}
func (k kvArrayType) Swap(i, j int)      { k[i], k[j] = k[j], k[i] }

func FIConsolidation(in MessageChannel, out MessageChannel) {
	pipe := func(fileNames ...string) {
		type storageType map[string]uint64
		type storageArrayType []storageType

		const maxLength = 100
		PositiveIntegerStorages := make(storageArrayType,maxLength)
		NegativeIntegerStorages := make(storageArrayType,maxLength)
		PositiveFloatStorages := make(storageArrayType,maxLength)
		NegativeFloatStorages := make(storageArrayType,maxLength)

		var storages *storageArrayType;
		var pathToFile string
		for _,fullFileName := range fileNames {
			if fullFileName == "" {
				continue
			}
			fileName := filepath.Base(fullFileName)
			pathToFile = filepath.Dir(fullFileName)
			pathToFile = strings.TrimSuffix(pathToFile,string(os.PathSeparator))
			pathToFile = filepath.Dir(pathToFile)
			dataLength, err := strconv.ParseInt(fileName, 10, 64)
			if err != nil {
				log.Println(fullFileName, " skipped")
				continue
			}
			zFile,err := os.Open(fullFileName)
			if err != nil {
				panic(err)
			}

			defer zFile.Close()
			dump,err := gzip.NewReader(zFile)
			if err != nil {
				panic(err)
			}
			data := bufio.NewScanner(dump)
			for data.Scan() {
				sval := data.Text()
				if strings.Contains(sval,".") {
					if strings.HasPrefix(sval,"-") {
						storages = &NegativeFloatStorages
					} else {
						storages = &PositiveFloatStorages
					}
				} else {
					if strings.HasPrefix(sval, "-") {
						storages = &NegativeIntegerStorages
					} else {
						storages = &PositiveIntegerStorages
					}
				}
				if (*storages)[dataLength] == nil {
					(*storages)[dataLength] = make(storageType)
				}
				(*storages)[dataLength][sval] = (*storages)[dataLength][sval] + 1
			}
		}
		sortAndSave := func(prefix string,storages *storageArrayType) {
			fieldSeparator := make([]byte,1)
			fieldSeparator[0] = byte(Conf.FieldSeparator)
			lineSeparator := make([]byte,1)
			lineSeparator[0] = byte(Conf.LineSeparator)

			for index,storage := range *storages {
				if storage != nil {
					kv := make(kvArrayType,0,len(storage))
					for k,v := range storage {
						kv = append(kv,kvType{key:k,value:fmt.Sprintf("%v",v)})
					}
					sort.Sort(kv)
					newPath := pathToFile + "/" + prefix
					os.Mkdir(newPath,0)
					newFileName := fmt.Sprintf("%v/%v",newPath,index)
					file,err := os.Create(newFileName)
					if err != nil {
						panic(err)
					}
					fmt.Println(newFileName)
					defer file.Close()
					zFile,err := gzip.NewWriterLevel(file,gzip.BestSpeed)
					if err != nil {
						panic(err)
					}
					defer zFile.Close()

					for _,data := range kv {
						zFile.Write([]byte(data.key))
						zFile.Write(fieldSeparator)
						zFile.Write([]byte(string(data.value)))
						zFile.Write(lineSeparator)
						//TODO:here must be calculation of if the dataset is a sequence
					}


				}
			}
		}
		sortAndSave("PI",&PositiveIntegerStorages)
		sortAndSave("NI",&NegativeIntegerStorages)
		sortAndSave("PF",&PositiveFloatStorages)
		sortAndSave("NF",&NegativeFloatStorages)
	}
		var dumpFileNameNI string
                for msg := range in {
		       if raw, found := (*msg)["I"]; found {
			       dumpFileName := raw.(string)
			       pipe(dumpFileName)
		       }
			if raw, found := (*msg)["F"]; found {
				dumpFileName := raw.(string)
				pipe(dumpFileName)
			}

			if raw, found := (*msg)["NI"]; found {
				dumpFileNameNI = raw.(string)
				dumpFileName := raw.(string)
				pipe(dumpFileName)
			}
			if raw, found := (*msg)["NF"]; found {
				dumpFileName := raw.(string)
				pipe(dumpFileNameNI,dumpFileName)
				dumpFileNameNI = ""
			}
			out <-msg
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
/*

{
  "dumpRootPath": "",
  "lineSeparatorByte": 10,
  "fieldSeparatorByte": 124,
  "h2-host": "localhost",
  "h2-port": "5435",
  "h2-dbname":"edm",
  "h2-login": "edm",
  "h2-password":"edmedm"
}v

*/
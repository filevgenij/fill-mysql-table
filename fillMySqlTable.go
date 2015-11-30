package main

import "fmt"
import "database/sql"
import _ "github.com/go-sql-driver/mysql"
import "strconv"
import "time"
import "encoding/json"
import "os"

type Interval struct {
    from int
    to int
}
// http://golang-book.ru/chapter-10-concurrency.html
// http://golang-book.ru/chapter-09-structs-and-interfaces.html
// https://astaxie.gitbooks.io/build-web-application-with-golang/content/en/05.2.html
// https://tour.golang.org/basics/15

type Config struct {
   ConString string
   FromTableName string
   ToTableName string
   Threads int
   PackSize int
}

var config Config

func main() {
    loadConfig()
    
    var c chan Interval = make(chan Interval)
    var r chan int = make(chan int)
    
    go responseWorker(r)
    for i := 1; i <= config.Threads; i++ {
        go processWorker(c, r);
    }
    commandWorker(c)
    
    time.Sleep(time.Second * 10)
    
    fmt.Println("Done!")
}

func loadConfig() {
    config = Config{}
    file, _ := os.Open("conf.json")
    decoder := json.NewDecoder(file)
    err := decoder.Decode(&config)
    if err != nil {
        fmt.Println("error:", err)
    }
}


func commandWorker(c chan Interval) {
    db, err := sql.Open("mysql", config.ConString + "?charset=utf8")
    checkErr(err);
    
    // query
    var maxId, cntRows int
    err = db.QueryRow("SELECT max(id) as maxId, count(*) as cntRows FROM " + config.FromTableName).Scan(&maxId, &cntRows)
    checkErr(err)
    
    fmt.Println("Total rows is " + strconv.Itoa(cntRows))
    
    i := 1
    // var from, to int;
    cnt := 0;
    for i <= maxId {
        interval := Interval{from: i, to: config.PackSize * (cnt + 1)}
        c <- interval
        
        // from = i
        // to = PackSize * (cnt + 1)
        //fmt.Println("[" + strconv.Itoa(from) +"; "+strconv.Itoa(to) +"]");
        i += config.PackSize
        cnt++
    }
    
    db.Close()
}

func processWorker(c chan Interval, r chan int) {
    db, err := sql.Open("mysql", config.ConString + "?charset=utf8")
    checkErr(err);
    var interval Interval
    
    // insert
    stmt, err := db.Prepare("INSERT INTO " + config.ToTableName + " SELECT * FROM " + config.FromTableName + " WHERE id BETWEEN ? AND ?")
    checkErr(err)

    for {
        interval = <- c
        _, err := stmt.Exec(interval.from, interval.to)
        checkErr(err)
        // fmt.Println((interval.to - interval.from), interval.to, interval.from)
        r <- (interval.to - interval.from + 1)
    }
    
    db.Close()
}

func responseWorker(r chan int) {
    total := 0;
    for {
        total += <- r
        fmt.Print("\r Inserted : " + strconv.Itoa(total) + " rows")
        //time.Sleep(time.Second * 1)
    }
}

func checkErr(err error) {
    if err != nil {
        panic(err)
    }
}
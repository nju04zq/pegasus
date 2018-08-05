package db

import (
	"database/sql"
	"pegasus/log"

	"github.com/go-gorp/gorp"
	_ "github.com/go-sql-driver/mysql"
)

const (
	MYSQL_NAME = "mysql"
	MYSQL_DSN  = "root:root@tcp(127.0.0.1:3306)/?charset=utf8"
)

func OpenMysqlDatabase(dbName string) (*Database, error) {
	d := &Database{
		driverName: MYSQL_NAME,
		dsn:        MYSQL_DSN,
		dbName:     dbName,
	}
	if err := d.open(); err != nil {
		return nil, err
	}
	return d, nil
}

type Database struct {
	driverName string
	dsn        string
	dbName     string
	db         *sql.DB
	dbmap      *gorp.DbMap
}

func (d *Database) open() (err error) {
	log.Info("Open database %s for %s", d.driverName, d.dbName)
	d.db, err = sql.Open(d.driverName, d.dsn)
	if err != nil {
		log.Error("Fail to open %s, %s, %v", d.driverName, d.dsn, err)
		return err
	}
	if err := d.prepareDb(d.dbName); err != nil {
		return err
	}
	d.dbmap = &gorp.DbMap{Db: d.db, Dialect: gorp.MySQLDialect{"InnoDB", "utf8mb4"}}
	return nil
}

func (d *Database) prepareDb(dbName string) error {
	log.Info("Prepare db %s", dbName)
	query := "CREATE DATABASE IF NOT EXISTS " + dbName
	if err := d.exec(query); err != nil {
		log.Error("Fail to prepare db %s, %v", dbName, err)
		return err
	}
	query = "USE " + dbName
	if err := d.exec(query); err != nil {
		return err
	}
	return nil
}

func (d *Database) exec(query string, args ...interface{}) error {
	if _, err := d.db.Exec(query, args...); err != nil {
		log.Error("Fail to exec query %s, args %v", query, args)
		return err
	}
	return nil
}

func (d *Database) GetDbmap() *gorp.DbMap {
	return d.dbmap
}

func (d *Database) Close() error {
	log.Info("Close ")
	if err := d.db.Close(); err != nil {
		log.Error("Fail to close db, %v", err)
		return err
	}
	return nil
}

package mgo

import (
	"gopkg.in/mgo.v2"
)

type Auth struct {
	Username     string
	Password     string
	Url          string
	DatabaseName string
	Mode         mgo.Mode
	Refresh      bool
}

func MongoConnection(credential Auth) (*mgo.Session, *mgo.Database, error) {
	session, _ := mgo.Dial(credential.Url)

	db := session.DB(credential.DatabaseName)
	err := db.Login(credential.Username, credential.Password)
	if err != nil {
		return nil, nil, err
	}
	session.SetMode(credential.Mode, credential.Refresh)
	return session, db, nil
}

func UseScheme(db *mgo.Database, nameScheme string) *mgo.Collection {

	return db.C(nameScheme)
}


/*
	READ ME
To use this wrapper, you have to create a structure, which will be the subject of the MongoConnection function,


MongoConnection returns three values; the first one is the connection session to the * mgo.Session type database, the second is  a * mgo.Database, and third will not be nil if you have got an error.


The UseScheme function takes an mgo.Database type (the second argument returned by MongoConnection) and returns a * mgo.Collection


*/

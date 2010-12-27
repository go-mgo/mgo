
var mongo = new Mongo("localhost:40201")
var admin = mongo.getDB("admin")
var dbs = admin.runCommand({"listDatabases": 1}).databases

for (var i = 0; i != dbs.length; i++) {
    var db = dbs[i]
    if (db.name != "admin" && db.name != "local") {
        mongo.getDB(db.name).dropDatabase()
    }
}

// vim:ts=4:sw=4:et

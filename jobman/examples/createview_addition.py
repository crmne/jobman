from jobman import sql

TABLE_NAME='test_add_'
db = sql.db('postgres://<user>:<pass>@<server>/<database>/'+TABLE_NAME)
db.createView(TABLE_NAME + 'view')

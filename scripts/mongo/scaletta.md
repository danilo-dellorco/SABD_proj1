# MongoDB (Document Data Store)

## Introduzione
Aggregato non opaco quindi posso avere documenti annidati e cose varie. MongoDB è tra i piu popolari nelle classifiche che introduce documenti a chiave univoca. Ogni documento puo avere schema differente. Documenti memorizzati in collection, che sono appunto insiemi di documenti.

La collection è utile quando abbiamo tantissimi documenti organizzarli in collezioni.

[chissà se michele ha scritto qualcosa...]

## MongoDB Setup

1. Let's retrieve the Docker image with pre-installed MongoDB
	(we use the official container)

	```
	docker pull mongo
	```

2. We can now create an isolated network with a MongoDB server

	```
	docker network create mongonet
	docker run -i -t -p 27017:27017 --name mongo_server --network=mongonet mongo:latest /usr/bin/mongod --bind_ip_all
	```


3. In another shell we can create a Mongo client, connected to the same network
	```
	docker run -i -t --name mongo_cli --network=mongonet mongo:latest /bin/bash
	```

4. On the client, we open the command line interface "mongo", specifying the address of the server
	```
	mongo mongo_server:27017
	```

5. We are ready to use MongoDB. We consider as use case an content management system, which needs to manage posts, images, videos, and so on, each with its own specific attributes; for example:
	```
	{ name : "hello world", type : "post", size : 250, comments : ["c1", "c2"] }

	{ name : "sunny day", type : "image", size : 3, url : "abc" }

	{ name : "tutorial", type : "video", length : 125, path : "/video.flv", metadata : {quality : "480p", color : "b/n", private : false } }
	```

## Basic Operations

### Create and switch to a new database named "cms":
```
use cms
```

### Insert in a collection named "cms" several documents (a post, an image, and a video)
```
db.cms.insert({ name : "hello world", type : "post", size : 250, comments : ["c1", "c2"] } )
db.cms.insert({ name : "sunny day", type : "image", size : 300, url : "abc" })
db.cms.insert({ name : "tutorial", type : "video", length : 125, path : "/video.flv", metadata : {quality : "480p", color : "b/n", private : false } })
```

### Querying the database to find documents
```
db.cms.find()

db.cms.find({type : "image" } )
db.cms.insert({ name : "mycms-logo", type : "image", size : 3, url : "cms-logo.jpg" })
db.cms.find({type : "image" } )

db.cms.find( {size : { $gt : 100 } } )
db.cms.find( {size : { $lt : 100 } } )

db.cms.find({ length : { $exists: true } } )
db.cms.find({ comments  : { $exists: true } } )
db.cms.find({ "comments.2"  : { $exists: true } } )
db.cms.find({ "comments.1"  : { $exists: true } } )

db.cms.find({ "metadata.quality" : { $exists: true } } )
db.cms.find({ "metadata.quality" : "360p" } )
db.cms.find({ "metadata.quality" : "480p" } )
db.cms.find({ "metadata.private" : false } )

db.cms.findOne({ comments  : { $exists: true } } )
db.cms.findOne({ comments  : { $exists: true } } ).comments[1]
```


### A slightly more complex query, that combines multiple conditions in a logical AND or OR condition:

```
AND:
	db.cms.find( {size : { $gt : 100 } , type : "image" } )

OR:
	db.cms.find( { $or : [ { size : { $gt : 100 } } , {  type : "image" } ] } )
```

### Sort results in ascending (value = 1) or descending order (value = -1)
```
db.cms.find().sort( { name : 1 } )
db.cms.find({type : "image" }).sort( { name : -1 } )
db.cms.find().sort( { nonexistingfield : 1 } )
```


### Update a document: there are several way to update a document; for example we can simply update a subset of fields using the operator $set:

```
db.cms.update({ "name" : "Canon EOS 750D" },  { $set: { "address.street": "East 31st Street" } } )
```

### Update a document: the previous command changes only the first occurrence; if the update should be performed on multiple occurrences, the parameter "multi" should be used:

```
db.cms.update({ "name" : "mycms-logo" },  { $set: { "metadata.quality": "hd" } } )
db.cms.find({name : "mycms-logo"})
db.cms.find({type : "image"})
db.cms.update({ type : "image" },  { $set: { "metadata.author": "myname" } } , {multi: true} )
db.cms.find({type : "image"})
```
### Update a document: how to add a new element to an array (using the operator $push):
```
db.cms.find({ type : "post" } )
db.cms.update({ name : "hello world" },  { $push: { "comments": "a new comment" } } )
db.cms.update({ name : "hello world" },  { $push: { "comments": "a second new comment" } } )
db.cms.find({ type : "post" } )
```

### Update a document: override the whole document
```
db.cms.find()
db.cms.update({ name : "mycms-logo" },  { author : "myname" } )
db.cms.find()
```

### Remove documents. By default .remove() removes all the documents that match the document fields passed as argument. If a single delete should be performed, we need to use the parameter "justOne".

```
db.cms.remove( { type : "video" } )
db.cms.remove( { author : "myname" } , { justOne: true } )

# the following removes all documents
db.cms.remove( { } )
```

### We can also drop the whole collection (together with its documents)
```
db.cms.drop()
```

#

*MEMO:* **Remember to stop containers and to remove the docker network! (see stop-containers.sh)**
const express = require("express");
const { MongoClient } = require("mongodb");
const morgan = require("morgan");
const cors = require("cors");
require("dotenv").config();

// app
const app = express();

// middlewares
app.use(morgan("dev"));
// add json perser
app.use(express.json());

app.use(cors());
let client;

const mongoClient = new MongoClient(process.env.DATABASE);
mongoClient.connect().then((connection) => {
    client = connection;
    console.log("Connected to MongoDB");
}).catch(err => {
    console.log(err);
});


// create get request
app.post("/:database/:collection", async (req, res) => {

    const database = req.params["database"];
    const collection = req.params["collection"];

    const db = client.db(database);

    let aggregationPipeline = []

    // POST http://localhost:8000/myDatabase/myCollection?search=iPhone 15
    if (req.query["search"]) {
        let searchText = req.query["search"];
        aggregationPipeline.push({ $search: { text: { query: searchText, path: { wildcard: "*" } } } });
    }
    
    
    // POST: http://localhost:8000/myDatabase/myCollection
    // Request Body
    // [
    //    {
    //        "field": "Price",
    //        "operator": ">",
    //        "value": "30"
    //    }
    // ]
    // string operators: contains, not contains, startswith, endswith, equals, not equals, isempty, isnotempty
    // numeric operators: =, !=, >, <, >=, <=, isempty, isnotempty
    // boolean operators: is
    // MongoDB Aggregation Pipeline: 
    // [ 
    //    { $match: 
    //        { $and: 
    //            [
    //                { Price: { $gt: 30 } },
    //                { Name: { $regex: ".*iPhone.*" } }
    //                { inStock: { $eq: true } }
    //            ]
    //        }
    //    }
    // ]

    let filters = []

    req.body.forEach(filter => {

        let field = filter.field;
        let operator = filter.operator;
        let value = filter.value;
        
        let query = {};

        switch (operator) {
            case "=":
                query[field] = Number(value)
                break;
            case "!=":
                query[field] = { $ne: Number(value) };
                break;
            case ">":
                query[field] = { $gt: Number(value) };
                break;
            case "<":
                query[field] = { $lt: Number(value) };
                break;
            case ">=":
                query[field] = { $gte: Number(value) };
                break;
            case "<=":
                query[field] = { $lte: Number(value) };
                break;
            case "contains":
                query[field] = { $regex: `.*${value}.*` };
                break;
            case "not contains":
                query[field] = { $not: { $regex: `.*${value}.*` } };
                break;
            case "startswith":
                query[field] = { $regex: `^${value}.*` };
                break;
            case "endswith":
                query[field] = { $regex: `.*${value}$` };
                break;
            case "equals":
                query[field] = { $eq: value };
                break;
            case "not equals":
                query[field] = { $ne: value };
                break;
            case "is empty":
                query[field] = { $eq: "" };
                break;
            case "is not empty":
                query[field] = { $ne: "" };
                break;
            case "is":
                query[field] = { $eq: value === 'true' };
                break;
            default:
                throw new Error("Invalid operator");
        }

        filters.push(query);
    });

    if(req.body.length > 0){
        aggregationPipeline.push({ $match: { $and: filters } });
    }
    


    // POST http://localhost:8000/myDatabase/myCollection?sort=Price asc
    if (req.query["sort"]) {
        let sortParams = req.query["sort"].split(" ");
        let sortColumn = sortParams[0];
        let sortOrder = sortParams[1] == "asc" ? 1 : -1;

        aggregationPipeline.push({ $sort: { [sortColumn]: sortOrder } });
    }

    // POST http://localhost:8000/myDatabase/myCollection?page=0&pageSize=10
    let page = parseInt(req.query["page"]) || 0;
    let pageSize = parseInt(req.query["pageSize"]) || 10;

    let skip = page * pageSize;

    // MongoDB Response
    // {
    //     "data": [
    //         { 
    //             "_id": 1,
    //             "Name": "iPhone 12",
    //         }
    //     ],
    //     "count": [
    //         { "total": 1 }
    //     ]
    // }
    aggregationPipeline.push({
        $facet: {
            data: [
                { $skip: skip },
                { $limit: pageSize }
            ],
            count: [
                { $count: "total" }
            ]
        }
    });

    const response = await db.collection(collection).aggregate(aggregationPipeline).next();

    const result = {
        data: response.data,
        total: response.count.length > 0 ? response.count[0].total : 0
    }
    res.json(result);
});

// Get the schema of the collection
app.get("/:database/:collection/schema", async (req, res) => {
    const database = req.params["database"];
    const collection = req.params["collection"];
    const db = client.db(database);

    let collectionDetails = await db.listCollections({ name: collection }).next();

    res.json(collectionDetails.options.validator);
})

// Add a new document to the collection
app.post("/:database/:collection/insert", async (req, res) => {
    const database = req.params["database"];
    const collection = req.params["collection"];
    const db = client.db(database);

    // Request Body
    // {
    //     "Name": "iPhone 12",
    //     "createdAt": "2021-10-01T00:00:00.000Z",
    //     "Price": 1000
    // }
    var cursor = db.listCollections({ name: collection });
    const collectionDetails = await cursor.next();
    const properties = collectionDetails.options.validator.$jsonSchema.properties;

    for (let key in req.body) {
        if (properties[key] != undefined && (properties[key]).bsonType === 'date') {
            req.body[key] = new Date(req.body[key]);
        }
    }

    const result = await db.collection(collection).insertOne(req.body);
    res.json(result);
});


app.put("/:database/:collection/:id", async (req, res) => {
    const database = req.params["database"];
    const collection = req.params["collection"];
    const db = client.db(database);
    const id = req.params["id"];


    const collectionDetails = await  db.listCollections({ name: collection }).next();
    const properties = collectionDetails.options.validator.$jsonSchema.properties;

    for (let key in req.body) {
        if (properties[key] != undefined && (properties[key]).bsonType === 'date') {
            req.body[key] = new Date(req.body[key]);
        }
    }

    const result = await db.collection(collection).updateOne({ _id: parseInt(id) }, { $set: req.body });
    res.json(result);
});


app.delete("/:database/:collection/:id", async (req, res) => {
    const database = req.params["database"];
    const collection = req.params["collection"];
    const db = client.db(database);
    const id = req.params["id"];

    const result = await db.collection(collection).deleteOne({ _id: parseInt(id) });
    res.json(result);
});

// Get all databases with collections
// {
//     "databases": [
//         {
//             "name": "weatherDB",
//             "collections": [
//                 "weather"
//             ]
//         }    
//     ]
// }

app.get("/", async (req, res) => {

    const dbInfo = await client.db().admin().listDatabases();
    const results = []
    for (db of dbInfo.databases) {
        if (db.name === "local" || db.name === "admin") continue;

        const collections = await client.db(db.name).listCollections({}, { nameOnly: true }).toArray();
        results.push(
            {
                databaseName: db.name,
                collections: collections.map(coll => coll.name)
            }
        )

    }

    res.json(results);
});

// exception handling middleware
app.use((err, req, res, next) => {

    const error = {
        status: err.statusCode || 500,
        message: err.message || 'Something went wrong'
    }
    
    if (err.code == 121) {
        error.status = 400;
        error.validationErrorDetails = err.errInfo.details;
    }

    res.status(error.status).json(error);
});

// http://localhost:8000/weatherDB/weather?$page=1&$pageSize=30&$sort=temperatureC&$filter=summary,contains,Hot&$filter=_id,%3C,8
// http://localhost:8000/weatherDB/weather?$page=1&$pageSize=30&$sort=temperatureC&$search=Warm

// port
const port = process.env.PORT || 8000;

app.listen(port, () => console.log(`Server is running on port ${port}`));
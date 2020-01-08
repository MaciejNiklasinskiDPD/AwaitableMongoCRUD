// Require MongoDB module.
const mongodb = require('mongodb');

// Get MongoClient, Db and ObjectID classes
const { MongoClient, ObjectID, Db, Cursor } = mongodb;

// Public methods

/**
 * Returns the database in asynchronous fashion.
 * @param {String} connectionURL The database connection url.
 * @param {String} databaseName The name of the database.
 * @param {Object=} options Optional settings.
 * @returns {Promise<Db>} The database instance.
 */
function getDatabaseAsync(connectionURL, databaseName, options) {
    // Returns the promise of obtaining the database reference.
    return new Promise((resolve, reject) => {
        // If options object has been provided .. 
        if (options)
            // Connect to MongoDB database
            MongoClient.connect(connectionURL, options, (error, client) => {
                // If error has been returned ..
                if (error)
                    // .. use it as a rejection argument while rejecting the promise.
                    reject(error);
                // Otherwise if no error has been returned ..
                else
                    // .. obtain the database reference and resolve the promise providing it as a completion argument.
                    resolve(client.db(databaseName));
            });
        // Otherwise if options parameter hasn't been provided
        else
            // Connect to MongoDB database
            MongoClient.connect(connectionURL, { useNewUrlParser: true, useUnifiedTopology: true }, (error, client) => {
                // If error has been returned ..
                if (error)
                    // .. use it as a rejection argument while rejecting the promise.
                    reject(error);
                // Otherwise if no error has been returned ..
                else
                    // .. obtain the database reference and resolve the promise providing it as a completion argument.
                    resolve(client.db(databaseName));
            });
    });
} module.exports.getDatabaseAsync = getDatabaseAsync;

/**
 * Inserts provided object to database in asynchronous fashion.
 * @param {Db} db Database to insert objectToInsert to.
 * @param {String} collectionKey The key of the collection the objectToInsert will be inserted to.
 * @param {Object} objectToInsert Object to be inserted to database.
 * @param {ObjectID=} _id The ObjectID under which object will be indexed within database. Optional, if not provided or null unique ID will be generated.
 * @param {Object=} options Optional settings.
 * @returns {Promise<CommandResult>} Returns a promise of inserting objectToInsert into the database.
 */
function insertOneAsync(db, collectionKey, objectToInsert, _id, options) {

    // If provided db is not an instance of mongodb Db class throw appropriate exception.
    if (!db instanceof Db) throw new TypeError('Provided argument \'db\' is not an instance of mongodb.Db class. Please provide instance of a valid type instead');
    // If provided collectionKey argument is not of expected type throw appropriate exception.    
    if (typeof (collectionKey) !== 'string') throw new TypeError('Provided \'collectionKey\' must be a string.');

    // If _id parameter has been provided ..
    if (_id) {
        // If provided _id is not an instance of ObjectID class throw appropriate exception.
        if (!_id instanceof ObjectID) throw new TypeError('Provided argument \'_id\' is not an instance of ObjectID class. Please provide instance of a valid type instead');

        // Add it as objectToInsert._id property value.
        objectToInsert._id = _id;
    }

    // Returns the promise of inserting the object into the database.
    return new Promise((resolve, reject) => {
        // If options object has been provided .. 
        if (options)
            // Inserts provided object to insert into the database under the provided collectionKey key.
            db.collection(collectionKey).insertOne(objectToInsert, options, (error, result) => {
                // If error has been returned ..
                if (error)
                    // .. use it as a rejection argument while rejecting the promise.
                    reject(error);
                // Otherwise if no error has been returned ..
                else
                    // .. resolve the promise using the insertion result as a completion argument.
                    resolve(result);
            });
        // Otherwise if options parameter hasn't been provided ..
        else
            // Inserts provided object to insert into the database under the provided collectionKey key.
            db.collection(collectionKey).insertOne(objectToInsert, (error, result) => {
                // If error has been returned ..
                if (error)
                    // .. use it as a rejection argument while rejecting the promise.
                    reject(error);
                // Otherwise if no error has been returned ..
                else
                    // .. resolve the promise using the insertion result as a completion argument.
                    resolve(result);
            });

    });
} module.exports.insertOneAsync = insertOneAsync;

/**
 * Inserts provided objects into the database in asynchronous fashion.
 * @param {Db} db Database to inserts objectsToInsert to. 
 * @param {String} collectionKey The key of the collection the objectsToInsert will be inserted to.
 * @param {[Object]} objectsToInsert Array of objects to be inserted to database.
 * @param {[ObjectID]=} _ids Array of ObjectID under which objects will be indexed within database. Optional, if not provided or null unique ID will be generated.
 * @param {Object=} options Optional settings.
 * @returns {Promise<CommandResult>} Returns a promise of inserting objectsToInsert into the database.
 */
function insertManyAsync(db, collectionKey, objectsToInsert, _ids, options) {

    // If provided db is not an instance of mongodb Db class throw appropriate exception.
    if (!db instanceof Db) throw new TypeError('Provided argument \'db\' is not an instance of mongodb.Db class. Please provide instance of a valid type instead');
    // If provided collectionKey argument is not of expected type throw appropriate exception.    
    if (typeof (collectionKey) !== 'string') throw new TypeError('Provided \'collectionKey\' must be a string.');
    // If provided argument objectsToInsert is not an array throw appropriate exception.
    if (!Array.isArray(objectsToInsert)) throw new TypeError('Provided argument \'objectsToInsert\' is not an Array.');

    // If _ids parameter has been provided validate it.
    if (_ids) {
        // If provided _ids argument is not an array throw appropriate exception.
        if (!Array.isArray(_ids)) throw new TypeError('Provided argument \'_ids\' is not an ObjectID[]. Please provide an array containing exclusively instances of ObjectID class and undefined.');
        // Loop through provided _ids ..
        _ids.forEach((_id) => {
            // .. and throw appropriate exception if any of Array entries is not an instance of ObjectID or undefined.
            if (!_id instanceof ObjectID && _id !== undefined) {
                throw new TypeError('Provided argument \'_ids\' is not an Array containing exclusively instances of ObjectID class and undefined.');
            }
        });

        // Loop through objectToInsert and insert ObjectID indexed in _ids array under matching index.
        for (var i = 0; i < objectsToInsert.length; i++) {
            // If currently looped through 'i' is greater then index of the last entry within _ids array, break out from the loop.
            if (i >= _ids.length) break;
            // If ObjectID indexed under currently looped through 'i' has been provided.
            if (_ids[i])
                // Add _id as objectToInsert._id property value
                objectsToInsert[i]._id = _ids[i];
        }
    }

    // Returns the promise of inserting array into the database.
    return new Promise((resolve, reject) => {
        if (options)
            // Inserts provided objects to insert into the database under the provided collectionKey key.
            db.collection(collectionKey).insertMany(objectsToInsert, options, (error, result) => {
                // If error has been returned ..
                if (error)
                    // .. use it as a rejection argument while rejecting the promise.
                    reject(error);
                // Otherwise if no error has been returned ..
                else
                    // .. resolve the promise using the insertion result as a completion argument.
                    resolve(result);
            });
        else
            // Inserts provided objects to insert into the database under the provided collectionKey key.
            db.collection(collectionKey).insertMany(objectsToInsert, (error, result) => {
                // If error has been returned ..
                if (error)
                    // .. use it as a rejection argument while rejecting the promise.
                    reject(error);
                // Otherwise if no error has been returned ..
                else
                    // .. resolve the promise using the insertion result as a completion argument.
                    resolve(result);
            });
    });
} module.exports.insertManyAsync = insertManyAsync;

/**
 * Searching through the database and returns the first entry matching provided selector filter.
 * @param {Db} db Database containing the collection to find an entry within.
 * @param {String} collectionKey The key of the collection containing the entry to find.
 * @param {Object} selector Filter by which a database entry will be filtered out.
 * @param {Object=} options Optional settings.
 * @returns {Promise<Object>} Database entry found based on the provided selector filter.
 */
function findOneAsync(db, collectionKey, selector, options) {
    // If provided db is not an instance of mongodb Db class throw appropriate exception.
    if (!db instanceof Db) throw new TypeError('Provided argument \'db\' is not an instance of mongodb.Db class. Please provide instance of a valid type instead');
    // If provided collectionKey argument is not of expected type throw appropriate exception.    
    if (typeof (collectionKey) !== 'string') throw new TypeError('Provided \'collectionKey\' must be a string.');

    // Returns promise of finding and returning a database entry.
    return new Promise((resolve, reject) => {
        // If options object has been provided .. 
        if (options)
            // Find first object matching provided selector filter.
            db.collection(collectionKey).findOne(selector, options, (error, result) => {
                // If error has been returned ..
                if (error)
                    // .. use it as a rejection argument while rejecting the promise.
                    reject(error);
                // Otherwise if no error has been returned ..
                else
                    // .. resolve the promise using the insertion result as a completion argument.
                    resolve(result);
            });
        // Otherwise if options object hasn't been provided ..
        else
            // Find first object matching provided selector filter.
            db.collection(collectionKey).findOne(selector, (error, result) => {
                // If error has been returned ..
                if (error)
                    // .. use it as a rejection argument while rejecting the promise.
                    reject(error);
                // Otherwise if no error has been returned ..
                else
                    // .. resolve the promise using the insertion result as a completion argument.
                    resolve(result);
            });
    });
} module.exports.findOneAsync = findOneAsync;

/**
 * Searching through the database and returns all the database cursor pointing to all the database entries matching provided selector filter.
 * @param {Db} db Database containing the collection to find an object within.
 * @param {String} collectionKey The key of the collection containing the entries to find.
 * @param {Object} selector Filter by which a database entries will be filtered out.
 * @param {Object=} options Optional settings.
 * @returns {Promise<Cursor>} Database entries found based on the provided selector filter.
 */
function findAsync(db, collectionKey, selector, options) {
    // If provided db is not an instance of mongodb Db class throw appropriate exception.
    if (!db instanceof Db) throw new TypeError('Provided argument \'db\' is not an instance of mongodb.Db class. Please provide instance of a valid type instead');
    // If provided collectionKey argument is not of expected type throw appropriate exception.    
    if (typeof (collectionKey) !== 'string') throw new TypeError('Provided \'collectionKey\' must be a string.');

    // Returns promise of finding and returning all database entries matching provided selector filter.
    return new Promise((resolve, reject) => {
        // If options object has been provided
        if (options)
            // Find all the entries matching provided selector filter.
            db.collection(collectionKey).find(selector, options, (error, result) => {
                // If error has been returned ..
                if (error)
                    // .. use it as a rejection argument while rejecting the promise.
                    reject(error);
                // Otherwise if no error has been returned ..
                else
                    // .. resolve the promise using the insertion result as a completion argument.
                    resolve(result);
            });
        // Otherwise if options object hasn't been provided
        else
            // Find all the entries matching provided selector filter.
            db.collection(collectionKey).find(selector, (error, result) => {
                // If error has been returned ..
                if (error)
                    // .. use it as a rejection argument while rejecting the promise.
                    reject(error);
                // Otherwise if no error has been returned ..
                else
                    // .. resolve the promise using the insertion result as a completion argument.
                    resolve(result);
            });
    });
} module.exports.findAsync = findAsync;

/**
 * Searching through the database and returns all entries matching provided selector filter.
 * @param {Db} db Database containing the collection to find an object within.
 * @param {String} collectionKey The key of the collection containing the entries to find.
 * @param {Object} selector Filter by which a database entries will be filtered out.
 * @param {Object=} options Optional settings.
 * @returns {Promise<[Object]>} Array of database entries found based on the provided selector filter.
 */
function findManyAsync(db, collectionKey, selector, options) {
    // If provided db is not an instance of mongodb Db class throw appropriate exception.
    if (!db instanceof Db) throw new TypeError('Provided argument \'db\' is not an instance of mongodb.Db class. Please provide instance of a valid type instead');
    // If provided collectionKey argument is not of expected type throw appropriate exception.    
    if (typeof (collectionKey) !== 'string') throw new TypeError('Provided \'collectionKey\' must be a string.');

    // Returns promise of finding and returning array containing all database entries matching provided selector filter.
    return new Promise(async (resolve, reject) => {

        // Declare cursor variable.
        let cursor = null;
        try {
            // Obtain database cursor to entires filtered out based on the provided selector. 
            cursor = await findAsync(db, collectionKey, selector, options);
            // If error has been returned ..
        } catch (error) {
            // .. use it as a rejection argument while rejecting the promise.
            reject(error);
        }

        // Obtains array on database entries from the database cursor.
        cursor.toArray((error, result) => {
            // If error has been returned ..
            if (error)
                // .. use it as a rejection argument while rejecting the promise.
                reject(error);
            // Otherwise ..
            else
                // .. resolve the promise using the insertion result as a completion argument.
                resolve(result);
        });
    });
} module.findManyAsync = findManyAsync;

/**
 * Updates the database entry found based on the provided selector filter.
 * @param {Db} db The database containing the entry to be updated.
 * @param {String} collectionKey The collection key of the collection containing the database entry.
 * @param {Object} selector The filter based on which database entry will be found.
 * @param {Object} updateQuery The query based on which database entries will be updated. Must be an object containing property named as one of mongodb update operators with value equal to non-null object.
 * @param {Object=} options Optional settings.
 * @returns {Promise<CommandResult>} Returns the promise of finding a database entry and updating it according provided updateQuery.
 */
function updateOneAsync(db, collectionKey, selector, updateQuery, options) {
    // If provided db is not an instance of mongodb Db class throw appropriate exception.
    if (!db instanceof Db) throw new TypeError('Provided argument \'db\' is not an instance of mongodb.Db class. Please provide instance of a valid type instead');
    // If provided collectionKey argument is not of expected type throw appropriate exception.    
    if (typeof (collectionKey) !== 'string') throw new TypeError('Provided \'collectionKey\' must be a string.');
    // If provided selector is not a non-null object throw appropriate exception.
    if (typeof selector !== 'object' || selector === null) throw new TypeError('Provided \'selector\' must be a non-null object.');
    // If provided updateQuery is not valid throw appropriate exception 
    if (!validateUpdateQuery(updateQuery))
        throw new TypeError(`Provided \'updateQuery\' must be an object with a property named as one of the valid mongodb update operators (${updateOperators}). The value of that property must be a non-null object.`);

    // Returns the promise of updating database entry.
    return new Promise((resolve, reject) => {
        // If options parameter has been provided ..
        if (options)
            // Update database entry found based on the provided selector.
            db.collection(collectionKey).updateOne(selector, updateQuery, options, (error, result) => {
                // If error has been returned ..
                if (error)
                    // .. use it as a rejection argument while rejecting the promise.
                    reject(error);
                // Otherwise ..
                else
                    // .. resolve the promise using the insertion result as a completion argument.
                    resolve(result);
            });
        // Otherwise if options parameter hasn't been provided ..
        else
            // Update database entry found based on the provided selector.
            db.collection(collectionKey).updateOne(selector, updateQuery, (error, result) => {
                // If error has been returned ..
                if (error)
                    // .. use it as a rejection argument while rejecting the promise.
                    reject(error);
                // Otherwise ..
                else
                    // .. resolve the promise using the insertion result as a completion argument.
                    resolve(result);
            });
    });
} module.exports.updateOneAsync = updateOneAsync;

/**
 * Updates the database entries found based on the provided selector filter.
 * @param {Db} db The database containing the entries to be updated.
 * @param {String} collectionKey The collection key of the collection containing the database entries.
 * @param {Object} selector The filter based on which database entries will be found.
 * @param {Object} updateQuery The query based on which database entries will be updated. Must be an object containing property named as one of mongodb update operators with value equal to non-null object.
 * @param {Object=} options Optional settings.
 * @returns {Promise<CommandResult>} Returns the promise of finding a database entries and updating them according provided updateQuery.
 */
function updateManyAsync(db, collectionKey, selector, updateQuery, options) {
    // If provided db is not an instance of mongodb Db class throw appropriate exception.
    if (!db instanceof Db) throw new TypeError('Provided argument \'db\' is not an instance of mongodb.Db class. Please provide instance of a valid type instead');
    // If provided collectionKey argument is not of expected type throw appropriate exception.    
    if (typeof (collectionKey) !== 'string') throw new TypeError('Provided \'collectionKey\' must be a string.');
    // If provided selector is not a non-null object throw appropriate exception.
    if (typeof selector !== 'object' || selector === null) throw new TypeError('Provided \'selector\' must be a non-null object.');
    // If provided updateQuery is not valid throw appropriate exception 
    if (!validateUpdateQuery(updateQuery))
        throw new TypeError(`Provided \'updateQuery\' must be an object with a property named as one of the valid mongodb update operators (${updateOperators}). The value of that property must be a non-null object.`);

    // Returns the promise of updating database entry.
    return new Promise((resolve, reject) => {
        // If options parameter has been provided ..
        if (options)
            // Update database entry found based on the provided selector.
            db.collection(collectionKey).updateMany(selector, updateQuery, options, (error, result) => {
                // If error has been returned ..
                if (error)
                    // .. use it as a rejection argument while rejecting the promise.
                    reject(error);
                // Otherwise ..
                else
                    // .. resolve the promise using the insertion result as a completion argument.
                    resolve(result);
            });
        // Otherwise if options parameter hasn't been provided
        else
            // Update database entry found based on the provided selector.
            db.collection(collectionKey).updateMany(selector, updateQuery, (error, result) => {
                // If error has been returned ..
                if (error)
                    // .. use it as a rejection argument while rejecting the promise.
                    reject(error);
                // Otherwise ..
                else
                    // .. resolve the promise using the insertion result as a completion argument.
                    resolve(result);
            });
    });
} module.exports.updateManyAsync = updateManyAsync;

/**
 * Deletes the database entry found based on the provided selector filter.
 * @param {Db} db The database containing the entry to be deleted.
 * @param {String} collectionKey The collection key of the collection containing the database entry.
 * @param {Object} selector The filter based on which database entry will be found.
 * @param {Object=} options Optional settings.
 * @return {Promise<CommandResult>} Returns a promise of removing the database entry.
 */
function deleteOneAsync(db, collectionKey, selector, options) {
    // Return the promise of removing the database entry.
    return new Promise((resolve, reject) => {
        // If options parameter has been provided ..
        if (options)
            // Remove database entry found based on the provided selector.
            db.collection(collectionKey).deleteOne(selector, options, (error, result) => {
                // If error has been returned ..
                if (error)
                    // .. use it as a rejection argument while rejecting the promise.
                    reject(error);
                // Otherwise ..
                else
                    // .. resolve the promise using the insertion result as a completion argument.
                    resolve(result);
            });
        // Otherwise if options parameter hasn't been provided
        else
            // Remove database entry found based on the provided selector.
            db.collection(collectionKey).deleteOne(selector, (error, result) => {
                // If error has been returned ..
                if (error)
                    // .. use it as a rejection argument while rejecting the promise.
                    reject(error);
                // Otherwise ..
                else
                    // .. resolve the promise using the insertion result as a completion argument.
                    resolve(result);
            });

    });
} module.exports.deleteOneAsync = deleteOneAsync;

/**
 * Deletes the database entries found based on the provided selector filter.
 * @param {Db} db The database containing the entries to be updated.
 * @param {String} collectionKey The collection key of the collection containing the database entries.
 * @param {Object} selector The filter based on which database entries will be found.
 * @param {Object=} options Optional settings.
 * @return {Promise<CommandResult>} Returns a promise of removing the database entries.
 */
function deleteManyAsync(db, collectionKey, selector, options) {
    // Return the promise of removing the database entries.
    return new Promise((resolve, reject) => {
        // If options parameter has been provided
        if (options)
            // Remove database entries found based on the provided selector.
            db.collection(collectionKey).deleteMany(selector, options, (error, result) => {
                // If error has been returned ..
                if (error)
                    // .. use it as a rejection argument while rejecting the promise.
                    reject(error);
                // Otherwise ..
                else
                    // .. resolve the promise using the insertion result as a completion argument.
                    resolve(result);
            });
        // Otherwise if options parameter hasn't been provided
        else
            // Remove database entries found based on the provided selector.
            db.collection(collectionKey).deleteMany(selector, (error, result) => {
                // If error has been returned ..
                if (error)
                    // .. use it as a rejection argument while rejecting the promise.
                    reject(error);
                // Otherwise ..
                else
                    // .. resolve the promise using the insertion result as a completion argument.
                    resolve(result);
            });
    });
} module.exports.deleteManyAsync = deleteManyAsync;



// Private Helpers


// Array containing all the valid update operators.
const updateOperators = ['$set', '$setOrInsert', '$unset', '$currentDate', '$inc', '$min', '$max', '$mul', '$mul', '$rename'];

/**
 * Answer's a question whether provided update query is valid.
 * @param {String} updateQuery Update quey to be validate.
 * @returns {Boolean} Returns true if provided update query is valid, otherwise returns false.
 */
function validateUpdateQuery(updateQuery) {

    // Loop through all the properties of the provided updateQuery object.
    for (key in updateQuery) {
        // If currently looped through property name is included within updateOperators array ..
        if (updateOperators.includes(key)
            // .. and the value of the property is non-null object ..
            && typeof updateQuery[key] === 'object' && updateQuery[key] !== null)
            // .. return true
            return true;
    }

    // Return false
    return false;
}
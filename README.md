# Div PHP Nodes 2.0.0
No-SQL Database System for PHP

Library for storage relational and serialized data 
using only PHP. The database can be divided into schemas. 
The objects (nodes) can be indexed for full text search 
and fast lookup.

## Introduction
Many PHP applications use relational databases to store and 
retrieve application information by connecting to SQL database servers.

Alternatively applications can also store information in file base 
databases that do not require the use of SQL, the so called noSQL databases.

This class provides a pure PHP implementation of a noSQL database that 
stores and retrieves information in files.

It provides features to avoid problems caused by concurrent accesses 
such as the use of proper file locking, among other the non-trivial 
file database access features.

## Description
This class can manage database of mixed values 
(atomic values, arrays, objects) named "nodes", stored in record files. 
It can create records that are stored in node files of a given schema 
directory.

Each node are serialized before storing the values in a file. Node 
files have a named defined by a identifier. Nodes may also reference 
other nodes in different schema directories.

The class may also:
- search for nodes that match conditions, 
- check if a node exists, 
- update, delete and rename nodes already stored in record files,
- lock node files to prevent changes
- get, add and remove node references,
- check if a schema directory exists, 
- create, rename and delete schema directories.
- iterate each node with closure function
- index the node's content for full text search
- store statistics, and update it dynamically 

## Documentation
https://github.com/divengine/nodes/wiki

## Installation

With composer...
```
composer require divengine/nodes
```

Without composer, download the class and...

```php
include "path/to/divengine/nodes.php";
```

## Basic usage

```php
<?php

use divengine/nodes;

$db = new nodes("database/contacts");

$id = $db->addNode([
    "name" => "Peter",
    "age" => 25
]);

$db->setNode($id, [
    "email" => "peter@email.com",
    "phone" => "+1222553335"
]);

$contact = $db->getNode($id);

$db->delNode($id);
```

Enjoy!

-- 

@rafageist

https://rafageist.com

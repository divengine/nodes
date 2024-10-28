# Div PHP Nodes

[![Latest Stable Version](https://poser.pugx.org/divengine/nodes/v)](https://packagist.org/packages/divengine/nodes) [![Total Downloads](https://poser.pugx.org/divengine/nodes/downloads)](https://packagist.org/packages/divengine/nodes) [![Latest Unstable Version](https://poser.pugx.org/divengine/nodes/v/unstable)](https://packagist.org/packages/divengine/nodes) [![License](https://poser.pugx.org/divengine/nodes/license)](https://packagist.org/packages/divengine/nodes) [![PHP Version Require](https://poser.pugx.org/divengine/nodes/require/php)](https://packagist.org/packages/divengine/nodes)

Div PHP Nodes is a PHP library for storing relational and serialized data without the need for an external server. The data is organized into schemas, and each object (or "node") can be indexed for full-text search and fast lookup.

This class manages file-based databases and provides mechanisms to avoid concurrency issues using file locking. Additionally, it allows:

- Creating, updating, deleting, renaming, and searching for nodes.
- Referencing nodes across different schemas.
- Iterating over nodes using closure functions.
- Indexing node content for quick searches.
- Storing and dynamically updating statistics.
- Managing schemas: creating, renaming, and deleting schema directories.

## Installation

With composer...

```bash
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

## Documentation

- [Divengine Software Solutions - Open Solutions - https://divengine.org](https://divengine.org)

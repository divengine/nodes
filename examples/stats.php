<?php

/**
 * Div PHP Nodes
 *
 * Example
 *
 * @author Rafa Rodriguez [@rafageist] <rafageist@hotmail.com>
 */
include "../divNodes.php";

// Adding a schema on the fly if not exists
$db = new divNodes("database/contacts");

// Deleting all nodes in schema database/contacts
$db->delNodes();

// Add nodes into schema
echo "Adding nodes...";
$db->addNode([
	"name" => "Peter Nash",
	"age" => 25,
	"city" => 'NY'
]);

$id = $db->addNode([
	"name" => "John Joseph",
	"age" => 33,
	"city" => 'FL'
]);

echo "Count of nodes before delete: {$db->getStats()['count']}\n";

$db->delNode($id);

echo "Count of nodes after delete: {$db->getStats()['count']}\n";

$db->createIndex(function($node)
{
	return $node['city'];
});

echo "Count of nodes with NY city: {$db->getStats('database/contacts/.index/n/y')['count']}\n";
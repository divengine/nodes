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

// Add node into schema database/contacts
$id = $db->addNode([
	"name" => "Peter Nash",
	"age" => 25,
	"city" => 'NY'
]);

$id = $db->addNode([
	"name" => "John Nash",
	"age" => 15,
	"city" => 'FL'
]);

// Drop old index
$db->delSchema('database/contacts/.index');

// Create index (in the default location) for full text search
$db->createIndex(function($node)
{
	return $node['name'];
});

// Full text search with 'jo nas' phrase
$results = $db->search("jo nas");

foreach ($results as $result)
{
	$node = $db->getNode($result['id'], $result['schema']);
	echo "[{$result['score']}] - " . $node['name'] . "\n";
}
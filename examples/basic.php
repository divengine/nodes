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

// Simple enums with PHP
class Enums {
	static public $cities = [
		'NY' => 'New York',
		'FL' => 'Florida',
		'HV' => 'Havana'
	];
}

// Deleting all nodes in schema database/contacts
$db->delNodes();

// Add node into schema database/contacts
$id = $db->addNode([
	"name" => "Peter Nash",
	"age" => 25,
	"city" => 'NY'
]);

// Change data of node
$db->setNode($id, [
	"email" => "peter@email.com",
	"phone" => "+1222553335"
]);

$id = $db->addNode(array(
	"name" => "John Nash",
	"age" => 15,
	"city" => 'FL'
));

// Retrieve a node from schema database/contacts
$contact = $db->getNode($id);

// Iterate each node with age < 20
$db->forEachNode(function(&$contact, $id, $schema, $db){

	if ($contact['age'] < 20)
		return DIV_NODES_FOR_CONTINUE_DISCARDING;

	if ( ! isset($contact['visitors']))
		$contact['visitors'] = 0;

	$contact['visitors']++;

	echo "Name: {$contact['name']} <br/>\n";
	echo "Phone: {$contact['phone']} <br/>\n";
	echo "Email: {$contact['email']} <br/>\n";
	echo "Visitors: {$contact['visitors']} <br/>\n";
	echo "\n";
});

// Drop index
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

// Create index of object's property
$idxCity = 'database/index/contacts/city';
$db->createIndex(function($node){
	if (isset($node['city']))
		return $node['city'];
	return 'unknown';
}, null, $idxCity);

$people_of_ny= $db->search('NY', $idxCity);

var_dump($people_of_ny);

// Remove node in current schema
$db->delNode($id);
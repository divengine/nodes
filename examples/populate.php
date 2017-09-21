<?php

/**
 * Div PHP Nodes
 *
 * Example
 *
 * @author Rafa Rodriguez [@rafageist] <rafageist@hotmail.com>
 */

include_once "../divNodes.php";

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

$id = $db->addNode(array(
	"name" => "John Nash",
	"age" => 15,
	"city" => 'FL'
));

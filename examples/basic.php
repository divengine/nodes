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

// Drop index
$db->delSchema('database/contacts/.index');

// Remove node in current schema
$db->delNode($id);
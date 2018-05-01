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
$db->addNode([
	"name" => "Peter Nash",
	"age" => 25,
	"city" => 'NY'
], "peter");

$db->addIndex(["ny"], "peter", null, null, true);

$db->renameNode("peter", "nash");

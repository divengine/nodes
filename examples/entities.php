<?php

/**
/**
 * Div PHP Nodes
 *
 * Example
 *
 * @author Rafa Rodriguez [@rafageist] <rafageist@hotmail.com>
 */

include "../divNodes.php";

// Your entity
class Person {
	public $first_name;
	public $last_name;
	public function getFullName() {
		return $this->first_name . " " . $this->last_name;
	}
}

// Clear schema
$db = new divNodes ( 'database' );

$db->delNodes ();

$person = new Person ();
$person->first_name = "John";
$person->last_name = "Nash";

// Save entity
$db->addNode ( $person );

$person = new Person ();

$person->first_name = "Albert";
$person->last_name = "Einstein";

// Save entity
$db->addNode ( $person );

$entities = $db->getNodes ( array (
		'order' => 'first_name' 
) );

foreach ( $entities as $e ) {
	echo $e->getFullName () . "<br/>\n";
}

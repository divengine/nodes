<?php

namespace divengine\nodes\tests\crud;

use PHPUnit\Framework\TestCase;
use divengine\nodes;

class BasicTest extends TestCase
{
    public $db;
    public $schema;
     
    public function setUp(): void
    {
        $this->schema = DATABASE_PATH . '/nodes';
        $this->db = new nodes($this->schema);
        $this->db->delNodes();
    }

    public function testNodes()
    {
        $this->assertInstanceOf(nodes::class, $this->db);
    }

    public function testAddNode()
    {
        $id = $this->db->addNode([
            "name" => "Peter Nash",
            "age" => 25,
            "city" => 'NY'
        ]);

        $this->assertIsString($id);

        // file exists
        $this->assertFileExists("{$this->schema}/nodes/$id");
    }

    public function testGetNode()
    {
        $id = $this->db->addNode([
            "name" => "Peter Nash",
            "age" => 25,
            "city" => 'NY'
        ]);

        $contact = $this->db->getNode($id);

        $this->assertIsArray($contact);
    }

    public function testSetNode()
    {
        $value = "peter@gmail.com";

        $id = $this->db->addNode([
            "name" => "Peter Nash",
            "age" => 25,
            "city" => 'NY'
        ]);

        $this->db->setNode($id, [
            "email" => $value
        ]);

        $contact = $this->db->getNode($id);
        
        $this->assertIsArray($contact);
        $this->assertArrayHasKey("email", $contact);
        $this->assertEquals($value, $contact["email"]);
        $this->assertIsString($contact["email"]);
    }

    public function testDelNode()
    {
        $id = $this->db->addNode([
            "name" => "Peter Nash",
            "age" => 25,
            "city" => 'NY'
        ]);

        $this->db->delNode($id);

        $this->assertFileDoesNotExist("{$this->schema}/nodes/$id");
    }

    // Tear down
    public function tearDown(): void
    {
        $this->db = new nodes("{$this->schema}/nodes/");
        $this->db->delNodes();
        $this->db->delSchema("{$this->schema}/nodes/");
    }
}